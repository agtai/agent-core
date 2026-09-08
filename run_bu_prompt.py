import asyncio
from openjiuwen.core.foundation.llm import init_model
from openjiuwen.harness.subagents import create_browser_agent

async def main():
    model = init_model()  # uses your usual env / settings
    agent = create_browser_agent(model, language="en")
    result = await agent.invoke({
        "query": "Open https://google.com and report the main heading test.",
        "conversation_id": "bu-lab-1",
    })
    print(result)

asyncio.run(main())