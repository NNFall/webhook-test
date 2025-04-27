from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/")
async def webhook_receiver(request: Request):
    data = await request.json()
    print("🚀 Пришел webhook:", data)
    return {"status": "ok"}

@app.get("/")
async def root():
    return {"message": "Сервер работает!"}
