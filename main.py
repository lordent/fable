from string.templatelib import Template

from fastapi import FastAPI

app = FastAPI()


@app.get("/hello/{name}")
async def use_tstring(name: str):
    greeting: Template = t"Привет, {name}!"

    return {
        "raw_template": greeting.strings,
        "interpolated_values": greeting.values,
        "result": str(greeting),
    }
