from typing import Any, Dict, List, Optional
import asyncio
import httpx
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass

@dataclass
class MetricAnalysis:
    timestamp: str
    severity: str
    message: str
    ratio: float

class PostgresAnalyzer:
    def __init__(self, api_base: str, username: str, password: str):
        self.api_base = api_base
        self.auth = httpx.BasicAuth(username, password)
        self.logger = logging.getLogger(__name__)

    def _get_tuple_efficiency_query(self) -> str:
        return """
        WITH base_data AS (
            SELECT
                m1.datname,
                m1.p_timestamp,
                m1.data_point_value as tup_returned,
                m2.data_point_value as tup_fetched
            FROM "pg-metrics" m1
            JOIN "pg-metrics" m2
                ON date_trunc('second', m1.p_timestamp) = date_trunc('second', m2.p_timestamp)
                AND m1.datname = m2.datname
            WHERE m1.metric_name = 'pg_stat_database_tup_returned'
            AND m2.metric_name = 'pg_stat_database_tup_fetched'
        ),
        rate_calc AS (
            SELECT
                datname,
                p_timestamp,
                (tup_returned - LAG(tup_returned) OVER (PARTITION BY datname ORDER BY p_timestamp)) /
                EXTRACT(EPOCH FROM (p_timestamp - LAG(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp))) as tup_returned_per_sec,
                (tup_fetched - LAG(tup_fetched) OVER (PARTITION BY datname ORDER BY p_timestamp)) /
                EXTRACT(EPOCH FROM (p_timestamp - LAG(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp))) as tup_fetched_per_sec
            FROM base_data
        )
        SELECT
            datname,
            date_trunc('hour', p_timestamp) as hourly_timestamp,
            ROUND(AVG(tup_returned_per_sec), 2) as avg_tuples_returned_per_sec,
            ROUND(AVG(tup_fetched_per_sec), 2) as avg_tuples_fetched_per_sec,
            CASE 
                WHEN AVG(tup_fetched_per_sec) > 0
                THEN ROUND((AVG(tup_returned_per_sec) / AVG(tup_fetched_per_sec))::numeric, 2)
                ELSE 0
            END as efficiency_ratio
        FROM rate_calc
        WHERE tup_returned_per_sec IS NOT NULL
        GROUP BY datname, date_trunc('hour', p_timestamp)
        ORDER BY hourly_timestamp DESC;
        """

    def _get_cache_hit_query(self) -> str:
        return """
        WITH base_data AS (
            SELECT
                m1.datname,
                m1.p_timestamp,
                m1.data_point_value as blocks_hit,
                m2.data_point_value as blocks_read
            FROM "pg-metrics" m1
            JOIN "pg-metrics" m2
                ON date_trunc('second', m1.p_timestamp) = date_trunc('second', m2.p_timestamp)
                AND m1.datname = m2.datname
            WHERE m1.metric_name = 'pg_stat_database_blks_hit'
            AND m2.metric_name = 'pg_stat_database_blks_read'
        ),
        rate_calc AS (
            SELECT
                datname,
                p_timestamp,
                (blocks_hit - LAG(blocks_hit) OVER (PARTITION BY datname ORDER BY p_timestamp)) /
                EXTRACT(EPOCH FROM (p_timestamp - LAG(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp))) as blocks_hit_per_sec,
                (blocks_read - LAG(blocks_read) OVER (PARTITION BY datname ORDER BY p_timestamp)) /
                EXTRACT(EPOCH FROM (p_timestamp - LAG(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp))) as blocks_read_per_sec
            FROM base_data
        )
        SELECT
            datname,
            date_trunc('hour', p_timestamp) as hourly_timestamp,
            ROUND(AVG(blocks_hit_per_sec), 2) as avg_blocks_hit_per_sec,
            ROUND(AVG(blocks_read_per_sec), 2) as avg_blocks_read_per_sec,
            CASE 
                WHEN (AVG(blocks_hit_per_sec) + AVG(blocks_read_per_sec)) > 0
                THEN ROUND((AVG(blocks_hit_per_sec) / (AVG(blocks_hit_per_sec) + AVG(blocks_read_per_sec)) * 100)::numeric, 2)
                ELSE 0
            END as cache_hit_ratio
        FROM rate_calc
        WHERE blocks_hit_per_sec IS NOT NULL
        GROUP BY datname, date_trunc('hour', p_timestamp)
        ORDER BY hourly_timestamp DESC;
        """

    def _analyze_efficiency_ratio(self, metrics: List[Dict[str, Any]]) -> List[MetricAnalysis]:
        analyses = []
        for row in metrics:
            ratio = row.get('efficiency_ratio', 0)
            if ratio == 0:
                continue
                
            timestamp = row.get('hourly_timestamp', 'unknown')
            if ratio <= 1.2:
                severity = "INFO"
                message = "Optimal index usage"
            elif ratio <= 2:
                severity = "INFO"
                message = "Good query performance"
            elif ratio <= 4:
                severity = "WARNING"
                message = "Suboptimal query patterns detected"
            else:
                severity = "CRITICAL"
                message = "Severe query inefficiency - possible full table scans"
            
            analyses.append(MetricAnalysis(timestamp, severity, message, ratio))
        return analyses

    def _analyze_cache_hits(self, metrics: List[Dict[str, Any]]) -> List[MetricAnalysis]:
        analyses = []
        for row in metrics:
            ratio = row.get('cache_hit_ratio', 0)
            if ratio == 0:
                continue
                
            timestamp = row.get('hourly_timestamp', 'unknown')
            if ratio >= 99:
                severity = "INFO"
                message = "Excellent cache performance"
            elif ratio >= 95:
                severity = "INFO"
                message = "Good cache utilization"
            elif ratio >= 90:
                severity = "WARNING"
                message = "Cache hit ratio below optimal"
            else:
                severity = "CRITICAL"
                message = "Poor cache performance - memory pressure likely"
            
            analyses.append(MetricAnalysis(timestamp, severity, message, ratio))
        return analyses

    def _get_efficiency_explanation(self, ratio: float) -> str:
        """
        Get a human-friendly explanation of the efficiency ratio.
        
        The ratio represents: (change in rows scanned) / (change in rows returned)
        measured over the same time interval. This indicates how many additional rows 
        PostgreSQL needs to scan internally for each additional row it returns.
        """
        if ratio <= 1.2:
            return (
                f"The efficiency ratio of {ratio:.1f} indicates excellent query performance. "
                "For each additional row returned, PostgreSQL is scanning approximately "
                "the same number of additional rows internally. This suggests optimal index usage and "
                "well-tuned queries."
            )
        elif ratio <= 2:
            return (
                f"The efficiency ratio of {ratio:.1f} shows good query performance. "
                f"For each additional row returned, PostgreSQL needs to scan about {ratio:.1f} additional rows. "
                "While not perfect, this indicates generally good index utilization. "
                "Some queries might benefit from minor optimizations."
            )
        elif ratio <= 4:
            return (
                f"The efficiency ratio of {ratio:.1f} indicates performance concerns. "
                f"PostgreSQL needs to scan {ratio:.1f} additional rows for each additional row returned. "
                "This suggests suboptimal query patterns or index usage. Common causes include:\n"
                "- Missing or suboptimal indexes\n"
                "- Complex conditions in WHERE clauses\n"
                "- Functions in WHERE clauses preventing index usage\n"
                "Consider reviewing query plans and index coverage."
            )
        else:
            return (
                f"The high efficiency ratio of {ratio:.1f} indicates serious performance issues. "
                f"PostgreSQL needs to scan {ratio:.1f} additional rows for each additional row returned. "
                "This typically indicates:\n"
                "- Full table scans instead of index scans\n"
                "- Missing indexes on frequently filtered columns\n"
                "- Complex joins without proper indexes\n"
                "- Use of functions in WHERE clauses (e.g., LOWER(), UPPER())\n"
                "Immediate query and index optimization is recommended."
            )

    def _get_cache_explanation(self, ratio: float) -> str:
        """Get a human-friendly explanation of the cache hit ratio"""
        if ratio >= 99:
            return (f"Cache hit ratio of {ratio:.1f}% is excellent. "
                   "Almost all data is being served from memory cache.")
        elif ratio >= 95:
            return (f"Cache hit ratio of {ratio:.1f}% is good. "
                   "Most data is being served from memory cache.")
        elif ratio >= 90:
            return (f"Cache hit ratio of {ratio:.1f}% is below optimal. "
                   "Consider increasing shared_buffers if possible.")
        else:
            return (f"Low cache hit ratio of {ratio:.1f}% indicates memory pressure. "
                   "Too much data is being read from disk instead of cache.")

    async def analyze_read_latency(self, time_range: str) -> str:
        """Analyze read latency patterns using both tuple efficiency and cache hit metrics"""
        async with httpx.AsyncClient(auth=self.auth) as client:
            # Calculate time range
            end_time = datetime.utcnow()
            value = int(''.join(filter(str.isdigit, time_range)))
            unit = ''.join(filter(str.isalpha, time_range))
            
            if unit == 'm':
                start_time = end_time - timedelta(minutes=value)
            elif unit == 'h':
                start_time = end_time - timedelta(hours=value)
            elif unit == 'd':
                start_time = end_time - timedelta(days=value)
            else:
                return "Invalid time range format"

            # Execute both queries
            url = f"{self.api_base}/api/v1/query"
            request_body = {
                "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            }

            try:
                # Get tuple efficiency metrics
                efficiency_response = await client.post(
                    url,
                    json={**request_body, "query": self._get_tuple_efficiency_query()},
                    timeout=60.0
                )
                efficiency_data = efficiency_response.json()
                
                # Get cache hit metrics
                cache_response = await client.post(
                    url,
                    json={**request_body, "query": self._get_cache_hit_query()},
                    timeout=60.0
                )
                cache_data = cache_response.json()

                # Analyze both metrics
                efficiency_analyses = self._analyze_efficiency_ratio(efficiency_data)
                cache_analyses = self._analyze_cache_hits(cache_data)

                # Compile findings with explanations
                findings = []
                
                # Add critical efficiency issues with explanations
                critical_efficiency = [a for a in efficiency_analyses if a.severity == "CRITICAL"]
                if critical_efficiency:
                    findings.append("\n🚨 Query Efficiency Issues:")
                    for finding in critical_efficiency[:3]:
                        findings.append(f"- Time: {finding.timestamp}")
                        findings.append(f"  {self._get_efficiency_explanation(finding.ratio)}")

                # Add critical cache issues with explanations
                critical_cache = [a for a in cache_analyses if a.severity == "CRITICAL"]
                if critical_cache:
                    findings.append("\n🚨 Cache Performance Issues:")
                    for finding in critical_cache[:3]:
                        findings.append(f"- Time: {finding.timestamp}")
                        findings.append(f"  {self._get_cache_explanation(finding.ratio)}")

                # Add summary if no critical issues
                if not findings:
                    recent_efficiency = efficiency_analyses[0] if efficiency_analyses else None
                    recent_cache = cache_analyses[0] if cache_analyses else None
                    
                    findings = ["📊 System Performance Summary:"]
                    if recent_efficiency:
                        findings.append("\nQuery Efficiency:")
                        findings.append(self._get_efficiency_explanation(recent_efficiency.ratio))
                    
                    if recent_cache:
                        findings.append("\nCache Performance:")
                        findings.append(self._get_cache_explanation(recent_cache.ratio))
                    
                    findings.append("\nGeneral Guidelines:")
                    findings.append("- Efficiency ratio ≤ 1.2: Optimal (perfect index usage)")
                    findings.append("- Efficiency ratio ≤ 2.0: Good (acceptable index usage)")
                    findings.append("- Efficiency ratio ≤ 4.0: Fair (room for optimization)")
                    findings.append("- Efficiency ratio > 4.0: Poor (needs immediate attention)")
                    findings.append("- Cache hit ratio ≥ 99%: Excellent")
                    findings.append("- Cache hit ratio ≥ 95%: Good")
                    findings.append("- Cache hit ratio < 90%: Needs attention")

                # Join findings into a single string and return
                return "\n".join(findings)

            except Exception as e:
                return f"Error analyzing read latency: {str(e)}"