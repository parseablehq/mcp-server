# src/mcp_parseable/server.py
import asyncio
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
import os
from dotenv import load_dotenv

from .utils.logger import setup_logger
from .analyzers.postgres import PostgresAnalyzer
from .workflows import Workflow, WorkflowType

# Initialize logging
logger = setup_logger()

# Load environment variables
load_dotenv()

PARSEABLE_API_BASE = os.environ.get("PARSEABLE_API_BASE")
P_USERNAME = os.environ.get("P_USERNAME")
P_PASSWORD = os.environ.get("P_PASSWORD")

logger.debug(f"API Base: {PARSEABLE_API_BASE}")
logger.debug(f"Username set: {'Yes' if P_USERNAME else 'No'}")
logger.debug(f"Password set: {'Yes' if P_PASSWORD else 'No'}")

# Initialize server and analyzer
server = Server("mcp_parseable")
analyzer = PostgresAnalyzer(PARSEABLE_API_BASE, P_USERNAME, P_PASSWORD)

async def dummy_write_latency():
    return "Write latency analysis (dummy): Everything looks good!"

async def dummy_storage_growth():
    return "Storage growth analysis (dummy): Normal growth patterns observed."

# Define available workflows
WORKFLOWS = {
    WorkflowType.READ_LATENCY: Workflow(
        type=WorkflowType.READ_LATENCY,
        description="Monitor PostgreSQL read query performance patterns",
        monitor_func=lambda: analyzer.analyze_read_latency("10m")
    ),
    WorkflowType.WRITE_LATENCY: Workflow(
        type=WorkflowType.WRITE_LATENCY,
        description="Monitor PostgreSQL write query performance patterns",
        monitor_func=dummy_write_latency
    ),
    WorkflowType.STORAGE_GROWTH: Workflow(
        type=WorkflowType.STORAGE_GROWTH,
        description="Monitor PostgreSQL storage growth patterns",
        monitor_func=dummy_storage_growth
    )
}

# src/mcp_parseable/server.py
# ... (previous imports stay the same)

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="list-workflows",
            description="List available monitoring workflows",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            },
        ),
        types.Tool(
            name="enable-workflow",
            description="Enable a specific monitoring workflow",
            inputSchema={
                "type": "object",
                "properties": {
                    "workflow_type": {
                        "type": "string",
                        "description": "Type of workflow to enable",
                        "enum": [w.value for w in WorkflowType]
                    }
                },
                "required": ["workflow_type"]
            },
        ),
        types.Tool(
            name="disable-workflow",
            description="Disable a specific monitoring workflow",
            inputSchema={
                "type": "object",
                "properties": {
                    "workflow_type": {
                        "type": "string",
                        "description": "Type of workflow to disable",
                        "enum": [w.value for w in WorkflowType]
                    }
                },
                "required": ["workflow_type"]
            },
        ),
        types.Tool(
            name="get-workflow-results",
            description="Get recent results from a specific workflow",
            inputSchema={
                "type": "object",
                "properties": {
                    "workflow_type": {
                        "type": "string",
                        "description": "Type of workflow to get results from",
                        "enum": [w.value for w in WorkflowType]
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of recent results to retrieve (default: 5)",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 5
                    }
                },
                "required": ["workflow_type"]
            },
        )
    ]

@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent]:
    logger.debug(f"Handling tool call: {name} with arguments: {arguments}")
    
    if name == "list-workflows":
        workflow_list = ["Available workflows:"]
        for workflow in WORKFLOWS.values():
            status = "🟢 Running" if workflow.is_running else "⚪️ Not running"
            result_count = len(workflow.results)
            workflow_list.append(f"- {workflow.type}: {workflow.description}")
            workflow_list.append(f"  Status: {status}, Results collected: {result_count}")
        return [types.TextContent(type="text", text="\n".join(workflow_list))]

    elif name == "enable-workflow":
        if not arguments or "workflow_type" not in arguments:
            raise ValueError("Missing workflow_type")
        
        workflow_type = WorkflowType(arguments["workflow_type"])
        workflow = WORKFLOWS[workflow_type]
        result = await workflow.start_monitoring()
        return [types.TextContent(type="text", text=result)]

    elif name == "disable-workflow":
        if not arguments or "workflow_type" not in arguments:
            raise ValueError("Missing workflow_type")
        
        workflow_type = WorkflowType(arguments["workflow_type"])
        workflow = WORKFLOWS[workflow_type]
        result = workflow.stop_monitoring()
        return [types.TextContent(type="text", text=result)]
    
    elif name == "get-workflow-results":
        if not arguments or "workflow_type" not in arguments:
            raise ValueError("Missing workflow_type")
        
        workflow_type = WorkflowType(arguments["workflow_type"])
        workflow = WORKFLOWS[workflow_type]
        limit = arguments.get("limit", 5)
        
        if not workflow.is_running:
            return [types.TextContent(type="text", text=f"Workflow {workflow_type} is not running")]
        
        recent_results = workflow.get_recent_results(limit)
        if not recent_results:
            return [types.TextContent(type="text", text=f"No results available for {workflow_type}")]
        
        results_text = [f"Recent results for {workflow_type}:"]
        for result in recent_results:
            timestamp = result.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
            results_text.append(f"\n[{timestamp}]")
            results_text.append(result.message)
        
        return [types.TextContent(type="text", text="\n".join(results_text))]
    
    else:
        raise ValueError(f"Unknown tool: {name}")

async def main():
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="mcp_parseable",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(
                        prompts_changed=True,
                        resources_changed=True,
                        tools_changed=True
                    ),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())