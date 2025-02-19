from typing import Any, Dict, List, Optional
import asyncio
import httpx
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass

@dataclass
class ReadMetricAnalysis:
    timestamp: str
    severity: str
    message: str
    ratio: float

@dataclass
class WriteMetricAnalysis:
    timestamp: str
    severity: str
    message: str
    write_rate: float
    breakdown: Dict[str, float]

@dataclass
class DBSizeAnalysis:
    database: str
    current_size_gb: float
    growth_gb: float
    growth_rate_mb_per_hour: float
    message: str

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
                ON m1.p_timestamp >= m2.p_timestamp - INTERVAL '1 second'
                AND m1.p_timestamp <= m2.p_timestamp + INTERVAL '1 second'
                AND m1.datname = m2.datname
            WHERE m1.metric_name = 'pg_stat_database_tup_returned'
            AND m2.metric_name = 'pg_stat_database_tup_fetched'
        )
        SELECT DISTINCT
            datname,
            FIRST_VALUE(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp DESC) as latest_timestamp,
            FIRST_VALUE(tup_returned) OVER (PARTITION BY datname ORDER BY p_timestamp DESC) as total_tuples_returned,
            FIRST_VALUE(tup_fetched) OVER (PARTITION BY datname ORDER BY p_timestamp DESC) as total_tuples_fetched,
            CASE 
                WHEN FIRST_VALUE(tup_fetched) OVER (PARTITION BY datname ORDER BY p_timestamp DESC) > 0
                THEN ROUND((FIRST_VALUE(tup_returned) OVER (PARTITION BY datname ORDER BY p_timestamp DESC)::float / 
                    FIRST_VALUE(tup_fetched) OVER (PARTITION BY datname ORDER BY p_timestamp DESC))::numeric, 2)
                ELSE 0
            END as efficiency_ratio
        FROM base_data
        ORDER BY latest_timestamp DESC;
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
                ON m1.p_timestamp >= m2.p_timestamp - INTERVAL '1 second'
                AND m1.p_timestamp <= m2.p_timestamp + INTERVAL '1 second'
                AND m1.datname = m2.datname
            WHERE m1.metric_name = 'pg_stat_database_blks_hit'
            AND m2.metric_name = 'pg_stat_database_blks_read'
        )
        SELECT DISTINCT
            datname,
            FIRST_VALUE(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp DESC) as latest_timestamp,
            FIRST_VALUE(blocks_hit) OVER (PARTITION BY datname ORDER BY p_timestamp DESC) as total_blocks_hit,
            FIRST_VALUE(blocks_read) OVER (PARTITION BY datname ORDER BY p_timestamp DESC) as total_blocks_read,
            CASE 
                WHEN (FIRST_VALUE(blocks_hit + blocks_read) OVER (PARTITION BY datname ORDER BY p_timestamp DESC)) > 0
                THEN ROUND((FIRST_VALUE(blocks_hit) OVER (PARTITION BY datname ORDER BY p_timestamp DESC)::float / 
                    FIRST_VALUE(blocks_hit + blocks_read) OVER (PARTITION BY datname ORDER BY p_timestamp DESC) * 100)::numeric, 2)
                ELSE 0
            END as cache_hit_ratio
        FROM base_data
        ORDER BY latest_timestamp DESC;
        """

    def _get_table_scan_query(self) -> str:
        return """
        WITH base_data AS (
            SELECT
                m1.datname,
                m1.relname,
                m1.p_timestamp,
                m1.data_point_value as seq_scans,
                m2.data_point_value as idx_scans
            FROM "pg-metrics" m1
            JOIN "pg-metrics" m2
                ON m1.p_timestamp >= m2.p_timestamp - INTERVAL '1 second'
                AND m1.p_timestamp <= m2.p_timestamp + INTERVAL '1 second'
                AND m1.datname = m2.datname
                AND m1.relname = m2.relname
            WHERE m1.metric_name = 'pg_stat_user_tables_seq_scan'
            AND m2.metric_name = 'pg_stat_user_tables_idx_scan'
        )
        SELECT DISTINCT
            datname,
            relname,
            FIRST_VALUE(seq_scans) OVER (PARTITION BY datname, relname ORDER BY p_timestamp DESC) as total_sequential_scans,
            FIRST_VALUE(idx_scans) OVER (PARTITION BY datname, relname ORDER BY p_timestamp DESC) as total_index_scans,
            ROUND((FIRST_VALUE(seq_scans) OVER (PARTITION BY datname, relname ORDER BY p_timestamp DESC)::float / 
                NULLIF(FIRST_VALUE(seq_scans) OVER (PARTITION BY datname, relname ORDER BY p_timestamp DESC) + 
                    FIRST_VALUE(idx_scans) OVER (PARTITION BY datname, relname ORDER BY p_timestamp DESC), 0) * 100), 2
            ) as sequential_scan_percentage
        FROM base_data
        ORDER BY sequential_scan_percentage DESC;
        """

    def _analyze_table_scans(self, metrics: List[Dict[str, Any]]) -> List[ReadMetricAnalysis]:
        analyses = []
        for row in metrics:
            seq_pct = row.get('sequential_scan_percentage', 0)
            if seq_pct == 0:
                continue
                
            table_name = f"{row.get('datname', 'unknown')}.{row.get('relname', 'unknown')}"
            if seq_pct <= 5:
                severity = "INFO"
                message = f"Good index usage on {table_name}"
            elif seq_pct <= 20:
                severity = "INFO"
                message = f"Acceptable scan patterns on {table_name}"
            elif seq_pct <= 50:
                severity = "WARNING"
                message = f"High sequential scans on {table_name}"
            else:
                severity = "CRITICAL"
                message = f"Excessive sequential scans on {table_name}"
            
            analyses.append(ReadMetricAnalysis(table_name, severity, message, seq_pct))
        return analyses

    def _get_scan_explanation(self, table_name: str, seq_pct: float, total_seq: int, total_idx: int) -> str:
        """Get a human-friendly explanation of the table scan patterns"""
        if seq_pct <= 5:
            return (
                f"Table {table_name} shows excellent index usage with only {seq_pct:.1f}% sequential scans. "
                f"Total scans: {total_seq} sequential, {total_idx} index-based."
            )
        elif seq_pct <= 20:
            return (
                f"Table {table_name} shows good scan patterns with {seq_pct:.1f}% sequential scans. "
                f"Total scans: {total_seq} sequential, {total_idx} index-based. "
                "Some sequential scans are normal for small tables or full table operations."
            )
        elif seq_pct <= 50:
            return (
                f"Table {table_name} has concerning scan patterns with {seq_pct:.1f}% sequential scans. "
                f"Total scans: {total_seq} sequential, {total_idx} index-based. "
                "Consider:\n"
                "- Adding indexes for common query patterns\n"
                "- Reviewing queries that might be forcing sequential scans\n"
                "- Checking if the table is small enough that sequential scans are actually optimal"
            )
        else:
            return (
                f"Table {table_name} shows problematic scan patterns with {seq_pct:.1f}% sequential scans. "
                f"Total scans: {total_seq} sequential, {total_idx} index-based. "
                "Urgent optimization needed:\n"
                "- Review and add missing indexes\n"
                "- Check for LIKE queries without proper indexes\n"
                "- Look for function calls in WHERE clauses\n"
                "- Verify statistics are up to date (ANALYZE)"
            )

    def _analyze_cache_hits(self, metrics: List[Dict[str, Any]]) -> List[ReadMetricAnalysis]:
        analyses = []
        for row in metrics:
            ratio = row.get('cache_hit_ratio', 0)
            if ratio == 0:
                continue
                
            timestamp = row.get('latest_timestamp', 'unknown')  # Fixed: using latest_timestamp
            if ratio >= 99:
                severity = "INFO"
                message = f"Excellent cache performance (hits: {row.get('total_blocks_hit', 0)}, reads: {row.get('total_blocks_read', 0)})"
            elif ratio >= 95:
                severity = "INFO"
                message = f"Good cache utilization (hits: {row.get('total_blocks_hit', 0)}, reads: {row.get('total_blocks_read', 0)})"
            elif ratio >= 90:
                severity = "WARNING"
                message = f"Cache hit ratio below optimal (hits: {row.get('total_blocks_hit', 0)}, reads: {row.get('total_blocks_read', 0)})"
            else:
                severity = "CRITICAL"
                message = f"Poor cache performance - memory pressure likely (hits: {row.get('total_blocks_hit', 0)}, reads: {row.get('total_blocks_read', 0)})"
            
            analyses.append(ReadMetricAnalysis(timestamp, severity, message, ratio))
        return analyses

    def _analyze_efficiency_ratio(self, metrics: List[Dict[str, Any]]) -> List[ReadMetricAnalysis]:
        analyses = []
        for row in metrics:
            ratio = row.get('efficiency_ratio', 0)
            if ratio == 0:
                continue
                
            timestamp = row.get('latest_timestamp', 'unknown')  # Fixed: using latest_timestamp
            if ratio <= 1.2:
                severity = "INFO"
                message = f"Optimal index usage (returned: {row.get('total_tuples_returned', 0)}, fetched: {row.get('total_tuples_fetched', 0)})"
            elif ratio <= 2:
                severity = "INFO"
                message = f"Good query performance (returned: {row.get('total_tuples_returned', 0)}, fetched: {row.get('total_tuples_fetched', 0)})"
            elif ratio <= 4:
                severity = "WARNING"
                message = f"Suboptimal query patterns detected (returned: {row.get('total_tuples_returned', 0)}, fetched: {row.get('total_tuples_fetched', 0)})"
            else:
                severity = "CRITICAL"
                message = f"Severe query inefficiency (returned: {row.get('total_tuples_returned', 0)}, fetched: {row.get('total_tuples_fetched', 0)})"
            
            analyses.append(ReadMetricAnalysis(timestamp, severity, message, ratio))
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
        """Analyze read latency patterns using tuple efficiency, cache hits, and table scans"""
        async with httpx.AsyncClient(auth=self.auth) as client:
            try:
                # Calculate time range
                end_time = datetime.utcnow()
                try:
                    value = int(''.join(filter(str.isdigit, time_range)))
                    unit = ''.join(filter(str.isalpha, time_range))
                    
                    if unit == 'm':
                        start_time = end_time - timedelta(minutes=value)
                    elif unit == 'h':
                        start_time = end_time - timedelta(hours=value)
                    elif unit == 'd':
                        start_time = end_time - timedelta(days=value)
                    else:
                        return f"Invalid time range format: {time_range}. Expected format: <number>[m|h|d]"
                except ValueError as e:
                    return f"Error parsing time range '{time_range}': {str(e)}"

                url = f"{self.api_base}/api/v1/query"
                request_body = {
                    "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                }

                # Get metrics with detailed error handling
                try:
                    efficiency_response = await client.post(
                        url,
                        json={**request_body, "query": self._get_tuple_efficiency_query()},
                        timeout=60.0
                    )
                    efficiency_response.raise_for_status()
                    efficiency_data = efficiency_response.json()
                except httpx.HTTPError as e:
                    self.logger.error(f"Error fetching efficiency metrics: {str(e)}")
                    return f"Error fetching efficiency metrics: {str(e)}\nStatus: {getattr(e.response, 'status_code', 'unknown')}\nResponse: {getattr(e.response, 'text', 'no response text')}"
                except Exception as e:
                    self.logger.error(f"Unexpected error fetching efficiency metrics: {str(e)}")
                    return f"Unexpected error fetching efficiency metrics: {str(e)}"
                
                try:
                    cache_response = await client.post(
                        url,
                        json={**request_body, "query": self._get_cache_hit_query()},
                        timeout=60.0
                    )
                    cache_response.raise_for_status()
                    cache_data = cache_response.json()
                except httpx.HTTPError as e:
                    self.logger.error(f"Error fetching cache metrics: {str(e)}")
                    return f"Error fetching cache metrics: {str(e)}\nStatus: {getattr(e.response, 'status_code', 'unknown')}\nResponse: {getattr(e.response, 'text', 'no response text')}"
                except Exception as e:
                    self.logger.error(f"Unexpected error fetching cache metrics: {str(e)}")
                    return f"Unexpected error fetching cache metrics: {str(e)}"
                
                try:
                    scan_response = await client.post(
                        url,
                        json={**request_body, "query": self._get_table_scan_query()},
                        timeout=60.0
                    )
                    scan_response.raise_for_status()
                    scan_data = scan_response.json()
                except httpx.HTTPError as e:
                    self.logger.error(f"Error fetching scan metrics: {str(e)}")
                    return f"Error fetching scan metrics: {str(e)}\nStatus: {getattr(e.response, 'status_code', 'unknown')}\nResponse: {getattr(e.response, 'text', 'no response text')}"
                except Exception as e:
                    self.logger.error(f"Unexpected error fetching scan metrics: {str(e)}")
                    return f"Unexpected error fetching scan metrics: {str(e)}"

                # Analyze metrics with error handling
                try:
                    efficiency_analyses = self._analyze_efficiency_ratio(efficiency_data)
                    cache_analyses = self._analyze_cache_hits(cache_data)
                    scan_analyses = self._analyze_table_scans(scan_data)
                except Exception as e:
                    self.logger.error(f"Error analyzing metrics: {str(e)}")
                    return f"Error analyzing metrics: {str(e)}\nEfficiency data: {efficiency_data}\nCache data: {cache_data}\nScan data: {scan_data}"

                findings = ["📊 PostgreSQL Performance Analysis"]
                
                # Add critical issues first
                critical_efficiency = [a for a in efficiency_analyses if a.severity == "CRITICAL"]
                critical_cache = [a for a in cache_analyses if a.severity == "CRITICAL"]
                critical_scans = [a for a in scan_analyses if a.severity == "CRITICAL"]
                
                if any([critical_efficiency, critical_cache, critical_scans]):
                    findings.append("\n🚨 Critical Issues Detected:")
                    
                    if critical_efficiency:
                        findings.append("\nQuery Efficiency Issues:")
                        for finding in critical_efficiency[:3]:
                            findings.append(f"- Time: {finding.timestamp}")
                            findings.append(f"  {self._get_efficiency_explanation(finding.ratio)}")
                    
                    if critical_cache:
                        findings.append("\nCache Performance Issues:")
                        for finding in critical_cache[:3]:
                            findings.append(f"- Time: {finding.timestamp}")
                            findings.append(f"  {self._get_cache_explanation(finding.ratio)}")
                    
                    if critical_scans:
                        findings.append("\nTable Scan Issues:")
                        for finding in critical_scans[:3]:
                            row = next((r for r in scan_data if f"{r['datname']}.{r['relname']}" == finding.timestamp), None)
                            if row:
                                findings.append(self._get_scan_explanation(
                                    finding.timestamp,
                                    finding.ratio,
                                    row['total_sequential_scans'],
                                    row['total_index_scans']
                                ))

                # Add summary of current state for all metrics
                findings.append("\n📈 Current Performance Summary:")
                
                # Cache performance summary
                findings.append("\nCache Performance:")
                for analysis in cache_analyses:
                    findings.append(f"- Time: {analysis.timestamp}")
                    findings.append(f"  {self._get_cache_explanation(analysis.ratio)}")
                
                # Efficiency summary
                if efficiency_analyses:
                    findings.append("\nQuery Efficiency:")
                    recent_efficiency = efficiency_analyses[0]
                    findings.append(self._get_efficiency_explanation(recent_efficiency.ratio))
                
                # Table scan summary
                if scan_analyses:
                    findings.append("\nTable Scan Patterns:")
                    for analysis in scan_analyses[:3]:  # Show top 3 most concerning scan patterns
                        row = next((r for r in scan_data if f"{r['datname']}.{r['relname']}" == analysis.timestamp), None)
                        if row:
                            findings.append(self._get_scan_explanation(
                                analysis.timestamp,
                                analysis.ratio,
                                row['total_sequential_scans'],
                                row['total_index_scans']
                            ))

                findings.append("\nPerformance Guidelines:")
                findings.append("Cache Hit Ratio:")
                findings.append("- ≥ 99%: Excellent - Optimal memory utilization")
                findings.append("- ≥ 95%: Good - Efficient cache usage")
                findings.append("- ≥ 90%: Fair - Room for optimization")
                findings.append("- < 90%: Poor - Needs immediate attention")
                
                findings.append("\nQuery Efficiency Ratio:")
                findings.append("- ≤ 1.2: Optimal (perfect index usage)")
                findings.append("- ≤ 2.0: Good (acceptable index usage)")
                findings.append("- ≤ 4.0: Fair (room for optimization)")
                findings.append("- > 4.0: Poor (needs immediate attention)")
                
                findings.append("\nSequential Scan Percentage:")
                findings.append("- ≤ 5%: Excellent")
                findings.append("- ≤ 20%: Good")
                findings.append("- > 50%: Needs immediate attention")

                return "\n".join(findings)

            except Exception as e:
                self.logger.error(f"Unexpected error in analyze_read_latency: {str(e)}")
                import traceback
                tb = traceback.format_exc()
                return f"Unexpected error in analyze_read_latency:\nError: {str(e)}\nTraceback:\n{tb}"

    def _get_write_latency_query(self) -> str:
        return """
        WITH base_data AS (
            SELECT
                m1.datname,
                m1.p_timestamp,
                m1.data_point_value as tup_inserted,
                m2.data_point_value as tup_updated,
                m3.data_point_value as tup_deleted
            FROM "pg-metrics" m1
            JOIN "pg-metrics" m2
                ON date_trunc('second', m1.p_timestamp) = date_trunc('second', m2.p_timestamp)
                AND m1.datname = m2.datname
            JOIN "pg-metrics" m3
                ON date_trunc('second', m1.p_timestamp) = date_trunc('second', m3.p_timestamp)
                AND m1.datname = m3.datname
            WHERE m1.metric_name = 'pg_stat_database_tup_inserted'
            AND m2.metric_name = 'pg_stat_database_tup_updated'
            AND m3.metric_name = 'pg_stat_database_tup_deleted'
        ),
        rate_calc AS (
            SELECT
                datname,
                p_timestamp,
                (tup_inserted - LAG(tup_inserted) OVER (PARTITION BY datname ORDER BY p_timestamp)) /
                EXTRACT(EPOCH FROM (p_timestamp - LAG(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp))) as inserts_per_sec,
                (tup_updated - LAG(tup_updated) OVER (PARTITION BY datname ORDER BY p_timestamp)) /
                EXTRACT(EPOCH FROM (p_timestamp - LAG(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp))) as updates_per_sec,
                (tup_deleted - LAG(tup_deleted) OVER (PARTITION BY datname ORDER BY p_timestamp)) /
                EXTRACT(EPOCH FROM (p_timestamp - LAG(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp))) as deletes_per_sec
            FROM base_data
        )
        SELECT
            datname,
            date_trunc('minute', p_timestamp) as minute_timestamp,
            ROUND(AVG(inserts_per_sec), 2) as avg_inserts_per_sec,
            ROUND(AVG(updates_per_sec), 2) as avg_updates_per_sec,
            ROUND(AVG(deletes_per_sec), 2) as avg_deletes_per_sec,
            ROUND(AVG(inserts_per_sec + updates_per_sec + deletes_per_sec), 2) as avg_total_writes_per_sec
        FROM rate_calc
        WHERE inserts_per_sec IS NOT NULL
        GROUP BY datname, date_trunc('minute', p_timestamp)
        ORDER BY minute_timestamp DESC
        LIMIT 100;
        """

    def _get_combined_write_query(self) -> str:
        return """
        WITH write_metrics AS (
            -- Original write metrics query
            WITH base_data AS (
                SELECT
                    m1.datname,
                    m1.p_timestamp,
                    m1.data_point_value as tup_inserted,
                    m2.data_point_value as tup_updated,
                    m3.data_point_value as tup_deleted
                FROM "pg-metrics" m1
                JOIN "pg-metrics" m2
                    ON date_trunc('second', m1.p_timestamp) = date_trunc('second', m2.p_timestamp)
                    AND m1.datname = m2.datname
                JOIN "pg-metrics" m3
                    ON date_trunc('second', m1.p_timestamp) = date_trunc('second', m3.p_timestamp)
                    AND m1.datname = m3.datname
                WHERE m1.metric_name = 'pg_stat_database_tup_inserted'
                AND m2.metric_name = 'pg_stat_database_tup_updated'
                AND m3.metric_name = 'pg_stat_database_tup_deleted'
            ),
            rate_calc AS (
                SELECT
                    datname,
                    p_timestamp,
                    (tup_inserted - LAG(tup_inserted) OVER (PARTITION BY datname ORDER BY p_timestamp)) /
                    EXTRACT(EPOCH FROM (p_timestamp - LAG(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp))) as inserts_per_sec,
                    (tup_updated - LAG(tup_updated) OVER (PARTITION BY datname ORDER BY p_timestamp)) /
                    EXTRACT(EPOCH FROM (p_timestamp - LAG(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp))) as updates_per_sec,
                    (tup_deleted - LAG(tup_deleted) OVER (PARTITION BY datname ORDER BY p_timestamp)) /
                    EXTRACT(EPOCH FROM (p_timestamp - LAG(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp))) as deletes_per_sec
                FROM base_data
            )
            SELECT
                datname,
                date_trunc('minute', p_timestamp) as minute_timestamp,
                ROUND(AVG(inserts_per_sec), 2) as avg_inserts_per_sec,
                ROUND(AVG(updates_per_sec), 2) as avg_updates_per_sec,
                ROUND(AVG(deletes_per_sec), 2) as avg_deletes_per_sec,
                ROUND(AVG(inserts_per_sec + updates_per_sec + deletes_per_sec), 2) as avg_total_writes_per_sec
            FROM rate_calc
            WHERE inserts_per_sec IS NOT NULL
            GROUP BY datname, date_trunc('minute', p_timestamp)
        ),
        bgwriter_metrics AS (
            -- BGWriter metrics query
            WITH buffer_data AS (
                SELECT
                    p_timestamp,
                    data_point_value as buffer_writes
                FROM "pg-metrics"
                WHERE metric_name = 'postgresql.bgwriter.buffers.writes'
                ORDER BY p_timestamp
            ),
            rate_calc AS (
                SELECT
                    p_timestamp,
                    (buffer_writes - LAG(buffer_writes) OVER (ORDER BY p_timestamp)) /
                    EXTRACT(EPOCH FROM (p_timestamp - LAG(p_timestamp) OVER (ORDER BY p_timestamp))) as writes_per_sec
                FROM buffer_data
            )
            SELECT
                date_trunc('minute', p_timestamp) as minute_timestamp,
                ROUND(AVG(writes_per_sec), 2) as avg_buffer_writes_per_sec,
                ROUND(MAX(writes_per_sec), 2) as max_buffer_writes_per_sec
            FROM rate_calc
            WHERE writes_per_sec IS NOT NULL
            GROUP BY date_trunc('minute', p_timestamp)
        )
        SELECT
            w.datname,
            w.minute_timestamp,
            w.avg_inserts_per_sec,
            w.avg_updates_per_sec,
            w.avg_deletes_per_sec,
            w.avg_total_writes_per_sec,
            COALESCE(b.avg_buffer_writes_per_sec, 0) as avg_buffer_writes_per_sec,
            COALESCE(b.max_buffer_writes_per_sec, 0) as max_buffer_writes_per_sec
        FROM write_metrics w
        LEFT JOIN bgwriter_metrics b ON date_trunc('minute', w.minute_timestamp) = b.minute_timestamp
        ORDER BY w.minute_timestamp DESC
        LIMIT 100;
        """

    def _analyze_write_patterns(self, metrics: List[Dict[str, Any]]) -> List[WriteMetricAnalysis]:
        if not metrics:
            return []  # Return empty list if no metrics data
            
        analyses = []
        for row in metrics:
            # Get values with defaults of 0 for null/missing values
            total_writes = row.get('avg_total_writes_per_sec') or 0
            buffer_writes = row.get('avg_buffer_writes_per_sec') or 0
            max_buffer_writes = row.get('max_buffer_writes_per_sec') or 0
            
            # Skip if no meaningful data
            if total_writes == 0 and buffer_writes == 0:
                continue

            timestamp = row.get('minute_timestamp', 'unknown')
            
            # Enhanced breakdown including bgwriter metrics
            breakdown = {
                'inserts': row.get('avg_inserts_per_sec') or 0,
                'updates': row.get('avg_updates_per_sec') or 0,
                'deletes': row.get('avg_deletes_per_sec') or 0,
                'buffer_writes': buffer_writes,
                'max_buffer_writes': max_buffer_writes
            }

            # Determine severity based on both write operations and bgwriter activity
            if total_writes <= 100 and buffer_writes <= 100:
                severity = "INFO"
                message = "Normal database write activity"
            elif total_writes <= 500 and buffer_writes <= 500:
                severity = "INFO"
                message = "Moderate write load with normal bgwriter activity"
            elif total_writes <= 1000 or buffer_writes <= 1000:
                severity = "WARNING"
                message = "High write volume - monitor system performance"
            else:
                severity = "CRITICAL"
                message = "Excessive write activity with high bgwriter load"

            # Add bgwriter-specific context to message
            if buffer_writes > 1000:
                message += f" (bgwriter: {buffer_writes:.1f} buffers/sec)"
            elif buffer_writes > total_writes * 2:
                message += " (high buffer write ratio - possible memory pressure)"

            analyses.append(WriteMetricAnalysis(
                timestamp=timestamp,
                severity=severity,
                message=message,
                write_rate=total_writes,
                breakdown=breakdown
            ))
        
        if not analyses:
            # Add a default analysis if no data was processed
            analyses.append(WriteMetricAnalysis(
                timestamp=datetime.utcnow().isoformat(),
                severity="INFO",
                message="No write activity detected in the specified time range",
                write_rate=0,
                breakdown={
                    'inserts': 0,
                    'updates': 0,
                    'deletes': 0,
                    'buffer_writes': 0,
                    'max_buffer_writes': 0
                }
            ))
        
        return analyses

    def _analyze_write_rates(self, metrics: List[Dict[str, Any]]) -> List[WriteMetricAnalysis]:
        analyses = []
        for row in metrics:
            total_writes = row.get('avg_total_writes_per_sec', 0)
            if total_writes == 0:
                continue

            timestamp = row.get('minute_timestamp', 'unknown')
            breakdown = {
                'inserts': row.get('avg_inserts_per_sec', 0),
                'updates': row.get('avg_updates_per_sec', 0),
                'deletes': row.get('avg_deletes_per_sec', 0)
            }

            # Analyze write patterns
            if total_writes <= 100:
                severity = "INFO"
                message = "Normal write activity"
            elif total_writes <= 500:
                severity = "INFO"
                message = "Moderate write load"
            elif total_writes <= 1000:
                severity = "WARNING"
                message = "High write volume - monitor system performance"
            else:
                severity = "CRITICAL"
                message = "Excessive write activity - potential performance impact"

            analyses.append(WriteMetricAnalysis(
                timestamp=timestamp,
                severity=severity,
                message=message,
                write_rate=total_writes,
                breakdown=breakdown
            ))
        return analyses

    def _get_write_pattern_explanation(self, rate: float, breakdown: Dict[str, float]) -> str:
        """Get a human-friendly explanation of write patterns"""
        total = rate
        inserts_pct = (breakdown['inserts'] / total * 100) if total > 0 else 0
        updates_pct = (breakdown['updates'] / total * 100) if total > 0 else 0
        deletes_pct = (breakdown['deletes'] / total * 100) if total > 0 else 0

        if total <= 100:
            return (
                f"Write activity is normal at {total:.1f} operations/sec "
                f"({inserts_pct:.1f}% inserts, {updates_pct:.1f}% updates, {deletes_pct:.1f}% deletes). "
                "This indicates healthy database write patterns."
            )
        elif total <= 500:
            return (
                f"Moderate write load at {total:.1f} operations/sec "
                f"({inserts_pct:.1f}% inserts, {updates_pct:.1f}% updates, {deletes_pct:.1f}% deletes). "
                "Consider:\n"
                "- Monitoring WAL generation rate\n"
                "- Checking if write patterns align with expected workload"
            )
        elif total <= 1000:
            return (
                f"High write volume at {total:.1f} operations/sec "
                f"({inserts_pct:.1f}% inserts, {updates_pct:.1f}% updates, {deletes_pct:.1f}% deletes). "
                "Recommendations:\n"
                "- Monitor checkpoint frequency and duration\n"
                "- Review WAL configuration settings\n"
                "- Consider write-heavy table partitioning\n"
                "- Evaluate bulk operation patterns"
            )
        else:
            return (
                f"Excessive write activity at {total:.1f} operations/sec "
                f"({inserts_pct:.1f}% inserts, {updates_pct:.1f}% updates, {deletes_pct:.1f}% deletes). "
                "Urgent actions needed:\n"
                "- Review and optimize batch operations\n"
                "- Check for unnecessary UPDATE triggers\n"
                "- Consider write distribution across replicas\n"
                "- Evaluate WAL compression settings\n"
                "- Monitor disk I/O saturation"
            )
    def _get_combined_explanation(self, rate: float, breakdown: Dict[str, float]) -> str:
        """Get a detailed explanation of both write patterns and bgwriter activity"""
        total = rate
        inserts_pct = (breakdown['inserts'] / total * 100) if total > 0 else 0
        updates_pct = (breakdown['updates'] / total * 100) if total > 0 else 0
        deletes_pct = (breakdown['deletes'] / total * 100) if total > 0 else 0
        buffer_writes = breakdown.get('buffer_writes', 0)
        max_buffer_writes = breakdown.get('max_buffer_writes', 0)

        if total <= 100 and buffer_writes <= 100:
            return (
                f"Write activity is normal at {total:.1f} operations/sec "
                f"({inserts_pct:.1f}% inserts, {updates_pct:.1f}% updates, {deletes_pct:.1f}% deletes) "
                f"with {buffer_writes:.1f} bgwriter buffers/sec. "
                "This indicates healthy database write patterns and good memory management."
            )
        elif total <= 500 and buffer_writes <= 500:
            return (
                f"Moderate write load at {total:.1f} operations/sec "
                f"({inserts_pct:.1f}% inserts, {updates_pct:.1f}% updates, {deletes_pct:.1f}% deletes) "
                f"with {buffer_writes:.1f} bgwriter buffers/sec.\n"
                "Consider monitoring:\n"
                "- WAL generation rate\n"
                "- Checkpoint frequency\n"
                "- Background writer statistics\n"
                "- Buffer cache hit ratio"
            )
        elif total <= 1000 or buffer_writes <= 1000:
            bgwriter_status = (
                f"Background writer is processing {buffer_writes:.1f} buffers/sec "
                f"(peak: {max_buffer_writes:.1f} buffers/sec)"
            )
            return (
                f"High write volume at {total:.1f} operations/sec "
                f"({inserts_pct:.1f}% inserts, {updates_pct:.1f}% updates, {deletes_pct:.1f}% deletes). "
                f"{bgwriter_status}\n"
                "Recommendations:\n"
                "- Review bgwriter_delay and bgwriter_lru_maxpages settings\n"
                "- Monitor checkpoint frequency and duration\n"
                "- Consider write-heavy table partitioning\n"
                "- Check for batch operations that could be spread out\n"
                "- Evaluate shared_buffers size"
            )
        else:
            buffer_pressure = ""
            if buffer_writes > max_buffer_writes * 0.8:
                buffer_pressure = " Background writer is near peak capacity."
            elif buffer_writes > total * 2:
                buffer_pressure = " High buffer write ratio indicates memory pressure."

            return (
                f"Critical write activity at {total:.1f} operations/sec "
                f"({inserts_pct:.1f}% inserts, {updates_pct:.1f}% updates, {deletes_pct:.1f}% deletes) "
                f"with {buffer_writes:.1f} bgwriter buffers/sec (peak: {max_buffer_writes:.1f}).{buffer_pressure}\n"
                "Urgent actions needed:\n"
                "- Review and optimize batch operations\n"
                "- Check bgwriter settings and memory configuration\n"
                "- Consider increasing checkpoint_timeout\n"
                "- Evaluate shared_buffers and effective_cache_size\n"
                "- Monitor disk I/O saturation\n"
                "- Consider distributing writes across time windows\n"
                "- Review WAL compression and synchronous_commit settings"
            )

    async def analyze_write_latency(self, time_range: str) -> str:
        """Analyze write latency patterns and bgwriter metrics to provide recommendations"""
        async with httpx.AsyncClient(auth=self.auth) as client:
            try:
                # Calculate time range
                end_time = datetime.utcnow()
                try:
                    value = int(''.join(filter(str.isdigit, time_range)))
                    unit = ''.join(filter(str.isalpha, time_range))
                    
                    if unit == 'm':
                        start_time = end_time - timedelta(minutes=value)
                    elif unit == 'h':
                        start_time = end_time - timedelta(hours=value)
                    elif unit == 'd':
                        start_time = end_time - timedelta(days=value)
                    else:
                        return f"Invalid time range format: {time_range}. Expected format: <number>[m|h|d]"
                except ValueError as e:
                    return f"Error parsing time range '{time_range}': {str(e)}"

                url = f"{self.api_base}/api/v1/query"
                request_body = {
                    "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "query": self._get_combined_write_query()  # Using the combined query
                }

                try:
                    response = await client.post(url, json=request_body, timeout=60.0)
                    response.raise_for_status()
                    metrics_data = response.json()
                except httpx.HTTPError as e:
                    self.logger.error(f"Error fetching metrics: {str(e)}")
                    return f"Error fetching metrics: {str(e)}"

                # Analyze patterns using the combined analysis
                analyses = self._analyze_write_patterns(metrics_data)
                
                findings = ["📝 PostgreSQL Write Pattern Analysis"]

                # Add critical issues first
                critical_issues = [a for a in analyses if a.severity == "CRITICAL"]
                if critical_issues:
                    findings.append("\n🚨 Critical Issues Detected:")
                    for finding in critical_issues[:3]:
                        findings.append(f"\nTime: {finding.timestamp}")
                        findings.append(self._get_combined_explanation(
                            finding.write_rate,
                            finding.breakdown
                        ))

                # Current state summary
                findings.append("\n📊 Current State:")
                if analyses:
                    recent = analyses[0]
                    findings.append(self._get_combined_explanation(
                        recent.write_rate,
                        recent.breakdown
                    ))

                findings.append("\n📈 Performance Guidelines:")
                findings.append("Write Operations (per second):")
                findings.append("- ≤ 100: Normal activity")
                findings.append("- ≤ 500: Moderate load")
                findings.append("- ≤ 1000: High volume (monitor closely)")
                findings.append("- > 1000: Excessive (needs attention)")
                
                findings.append("\nBackground Writer (buffers/second):")
                findings.append("- ≤ 100: Optimal performance")
                findings.append("- ≤ 500: Normal load")
                findings.append("- ≤ 1000: High load (review settings)")
                findings.append("- > 1000: Performance critical (immediate action required)")

                return "\n".join(findings)

            except Exception as e:
                self.logger.error(f"Unexpected error in analyze_write_latency: {str(e)}")
                import traceback
                tb = traceback.format_exc()
                return f"Unexpected error in analyze_write_latency:\nError: {str(e)}\nTraceback:\n{tb}"

    def _get_db_size_query(self) -> str:
        return """
        WITH size_data AS (
            SELECT
                datname,
                p_timestamp,
                data_point_value as size_bytes
            FROM "pg-metrics"
            WHERE metric_name = 'pg_database_size_bytes'
        )
        SELECT
            datname,
            FIRST_VALUE(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp DESC) as current_timestamp,
            FIRST_VALUE(size_bytes) OVER (PARTITION BY datname ORDER BY p_timestamp DESC) as current_size_bytes,
            FIRST_VALUE(size_bytes) OVER (PARTITION BY datname ORDER BY p_timestamp ASC) as initial_size_bytes,
            ROUND(
                (FIRST_VALUE(size_bytes) OVER (PARTITION BY datname ORDER BY p_timestamp DESC) - 
                FIRST_VALUE(size_bytes) OVER (PARTITION BY datname ORDER BY p_timestamp ASC)) /
                NULLIF(EXTRACT(EPOCH FROM (
                    FIRST_VALUE(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp DESC) - 
                    FIRST_VALUE(p_timestamp) OVER (PARTITION BY datname ORDER BY p_timestamp ASC)
                )), 0),
                2
            ) as growth_rate_bytes_per_sec
        FROM size_data
        ORDER BY current_timestamp DESC;
        """
    
    def _analyze_db_size(self, metrics: List[Dict[str, Any]]) -> List[DBSizeAnalysis]:
        analyses = []
        for row in metrics:
            current_bytes = row.get('current_size_bytes', 0)
            initial_bytes = row.get('initial_size_bytes', 0)
            growth_rate = row.get('growth_rate_bytes_per_sec', 0)
            
            # Convert to more readable units
            current_gb = current_bytes / (1024 * 1024 * 1024)
            growth_gb = (current_bytes - initial_bytes) / (1024 * 1024 * 1024)
            growth_rate_mb_per_hour = (growth_rate * 3600) / (1024 * 1024)
            
            # Calculate time until critical thresholds
            hours_until_double = (current_bytes / growth_rate) / 3600 if growth_rate > 0 else float('inf')
            
            message = (
                f"Database: {row.get('datname', 'unknown')}\n"
                f"Current Size: {current_gb:.2f} GB\n"
                f"Total Growth: {growth_gb:.2f} GB\n"
                f"Growth Rate: {growth_rate_mb_per_hour:.2f} MB/hour"
            )
            
            if hours_until_double < float('inf'):
                days_until_double = hours_until_double / 24
                message += f"\nAt this rate, size will double in {days_until_double:.1f} days"

            analyses.append(DBSizeAnalysis(
                database=row.get('datname', 'unknown'),
                current_size_gb=current_gb,
                growth_gb=growth_gb,
                growth_rate_mb_per_hour=growth_rate_mb_per_hour,
                message=message
            ))
        
        return analyses

    async def analyze_db_size(self, time_range: str) -> str:
        """Analyze database size growth patterns"""
        async with httpx.AsyncClient(auth=self.auth) as client:
            try:
                # Calculate time range
                end_time = datetime.utcnow()
                try:
                    value = int(''.join(filter(str.isdigit, time_range)))
                    unit = ''.join(filter(str.isalpha, time_range))
                    
                    if unit == 'm':
                        start_time = end_time - timedelta(minutes=value)
                    elif unit == 'h':
                        start_time = end_time - timedelta(hours=value)
                    elif unit == 'd':
                        start_time = end_time - timedelta(days=value)
                    else:
                        return f"Invalid time range format: {time_range}. Expected format: <number>[m|h|d]"
                except ValueError as e:
                    return f"Error parsing time range '{time_range}': {str(e)}"

                url = f"{self.api_base}/api/v1/query"
                request_body = {
                    "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "query": self._get_db_size_query()
                }

                try:
                    response = await client.post(url, json=request_body, timeout=60.0)
                    response.raise_for_status()
                    metrics_data = response.json()
                except httpx.HTTPError as e:
                    self.logger.error(f"Error fetching metrics: {str(e)}")
                    return f"Error fetching metrics: {str(e)}"

                analyses = self._analyze_db_size(metrics_data)
                
                if not analyses:
                    return "No database size metrics available for the specified time range."

                findings = ["📊 PostgreSQL Database Size Analysis"]
                
                # Sort databases by growth rate
                analyses.sort(key=lambda x: x.growth_rate_mb_per_hour, reverse=True)
                
                # Add analysis for each database
                for analysis in analyses:
                    findings.append(f"\n{analysis.message}")
                    
                    # Add recommendations based on growth rate
                    if analysis.growth_rate_mb_per_hour > 1000:  # More than 1GB/hour
                        findings.append("\nRecommendations for high growth rate:")
                        findings.append("- Review and optimize data retention policies")
                        findings.append("- Consider table partitioning")
                        findings.append("- Plan for storage capacity increase")
                        findings.append("- Monitor WAL generation rate")
                        findings.append("- Check for bloat and consider vacuum")
                    elif analysis.growth_rate_mb_per_hour > 100:  # More than 100MB/hour
                        findings.append("\nRecommendations:")
                        findings.append("- Monitor growth trends")
                        findings.append("- Review data lifecycle policies")
                        findings.append("- Plan future storage needs")

                return "\n".join(findings)

            except Exception as e:
                self.logger.error(f"Unexpected error in analyze_db_size: {str(e)}")
                import traceback
                tb = traceback.format_exc()
                return f"Unexpected error in analyze_db_size:\nError: {str(e)}\nTraceback:\n{tb}"