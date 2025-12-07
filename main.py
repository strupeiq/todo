import asyncio
from http.client import HTTPException
import time
from fastapi import FastAPI, Request, Depends, WebSocket, WebSocketDisconnect
from fastapi import BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import json
from nats.aio.client import Client as NATSClient
import nats

from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlmodel import SQLModel, Field
from playwright.async_api import async_playwright

Base = declarative_base()
engine = create_async_engine(
    "sqlite+aiosqlite:///./tasks.db"
)
DBSession = async_sessionmaker(bind=engine, autoflush=False,
                         autocommit=False,
                         class_=AsyncSession)
NATSClientType = NATSClient


class ConnectionManager:
    def __init__(self):
        self.active_connetcion: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connetcion.append(websocket)

    async def handle(self, data, websocket: WebSocket):
        if data == "spec":
            await websocket.send_text("SPEC OK!")
        elif data == "close":
            await self.disconnect(websocket)
        else:
            await websocket.send_text(data * 10)

    async def disconnect(self, websocket: WebSocket):
        await websocket.close()
        self.active_connetcion.remove(websocket)

    async def broadcast(self, data: str):
        for ws in self.active_connetcion:
            await ws.send_text(data)

    async def broadcast_json(self, data: dict):
        await self.broadcast(json.dumps(data))


manager = ConnectionManager()


class TaskEvent(BaseModel):
    event_type: str
    task_id: int | None = Field(primary_key=True)
    task_title: str
    timestamp: float


class NatsManager:
    def __init__(self):
        self.nc: NATSClient | None = None

    async def connect(self):
        try:
            self.nc = await nats.connect("nats://localhost:4222")
            print("Подключение к NATS установлено")
            await self.nc.subscribe("task_events", cb=self.handle_nats_message)

        except Exception as e:
            print(f"Ошибка подключения к NATS: {e}")
            self.nc = None

    async def publish_task_event(self, event: TaskEvent):
        if self.nc:
            try:
                event_data = json.dumps(event.dict())
                await self.nc.publish("task_events", event_data.encode())
                print(f"Событие отправлено в NATS: {event.event_type} task {event.task_id}")
            except Exception as e:
                print(f"Ошибка отправки в NATS: {e}")

    async def handle_nats_message(self, msg):
        try:
            data = msg.data.decode()
            await manager.broadcast(data)
        except Exception as e:
            print(f"Ошибка обработки сообщения NATS: {e}")


nats_manager = NatsManager()


class TaskModel(SQLModel, table=True):
    __tablename__ = "tasks"
    id: int | None = Field(primary_key=True)
    title: str
    description: str
    done: bool = False


async def get_db():
    db = DBSession()
    try:
        yield db
    finally:
        await db.close()


app = FastAPI(
    title="TODO API",
    version="1.0"
)


@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(
            SQLModel.metadata.create_all
        )
    await nats_manager.connect()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.middleware("http")
async def log_requwsts(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    print(
        f"Request to {request.url.path} processed in {process_time:.4f} seconds")
    return response


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)

    async def tick():
        while True:
            websocket.send_text("FOO!")
            await asyncio.sleep(1)
    asyncio.current_task(tick())
    try:
        while True:
            data = await websocket.receive_text()
            await manager.handle(data, websocket)
    except WebSocketDisconnect:
        await manager.disconnect(websocket)


async def send_task_event(event_type: str, task: TaskModel):
    event = TaskEvent(
        event_type=event_type,
        task_id=task.id,
        task_title=task.title,
        timestamp=time.time()
    )
    await nats_manager.publish_task_event(event)


@app.get("/add")
def add_numbers(a: int, b: int):
    return {"result": a + b}


class TaskCreate(BaseModel):
    title: str
    description: str


class TaskUpdate(TaskCreate):
    title: str
    description: str
    done: bool = False


class Task(TaskUpdate):
    id: int

    class Config:
        from_attributes = True


tasks: list[Task] = []
next_id = 1


@app.get("/tasks", response_model=list[Task])
async def get_tasks(db: AsyncSession = Depends(get_db)):
    stmt = select(TaskModel)
    result = await db.execute(stmt)
    tasks = result.scalars().all()
    return tasks


@app.get("/tasks/{task_id}", response_model=Task)
async def get_task(task_id: int, db: AsyncSession = Depends(get_db)):
    stmt = select(TaskModel).where(TaskModel.id == task_id)
    result = await db.execute(stmt)
    task = result.scalar_one_or_none()

    if not task:
        raise HTTPException(
            status_code=404,
            detail="Task not found"
        )
    return task


@app.post("/tasks", response_model=Task, status_code=201)
async def create_task(task: TaskCreate, db: AsyncSession = Depends(get_db)):
    new_task = TaskModel(
        title=task.title,
        description=task.description,
        done=False
    )
    db.add(new_task)
    await db.commit()
    await db.refresh(new_task)

    await send_task_event("created", new_task)

    return new_task


@app.put("/tasks/{task_id}", response_model=Task)
async def update_task(task_id: int, updated: TaskUpdate, db: AsyncSession = Depends(get_db)):
    stmt = select(TaskModel).where(TaskModel.id == task_id)
    result = await db.execute(stmt)
    task = result.scalar_one_or_none()

    if not task:
        raise HTTPException(
            status_code=404,
            detail="Task not found"
        )

    old_title = task.title
    old_description = task.description

    task.title = updated.title
    task.description = updated.description
    task.done = updated.done
    await db.commit()
    await db.refresh(task)

    await send_task_event("updated", task)

    return task


@app.delete("/tasks/{task_id}", status_code=204)
async def delete_task(task_id: int, db: AsyncSession = Depends(get_db)):
    obj = await db.get(TaskModel, task_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Task not found")

    task_data = {"id": obj.id, "title": obj.title}

    await db.delete(obj)
    await db.commit()

    event = TaskEvent(
        event_type="deleted",
        task_id=task_data["id"],
        task_title=task_data["title"],
        timestamp=time.time()
    )
    await nats_manager.publish_task_event(event)


@app.get("/async_task")
async def async_task():
    await asyncio.sleep(60)
    return {"message": "ok"}


@app.get("/background_task")
async def background_task(background_task: BackgroundTasks):
    def slow_time():
        import time

        time.sleep(60)

    background_task.add_task(slow_time)
    return {"message": "task started"}


executor = ThreadPoolExecutor(max_workers=2)
executor = ProcessPoolExecutor(max_workers=2)


def blocking_io_task():
    import time

    time.sleep(60)
    return "ok"


@app.get("/thread_pool_sleep")
async def thread_pool_sleep():
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(executor, blocking_io_task)
    return {"message": result}


def heavy_func(n: int):
    result = 0
    for i in range(n):
        result += i * i
    return result


@app.get("/cpu_task")
async def cpu_task(n: int = 10_000_000_000):
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(executor, heavy_func, n)
    return {"message": result}


class Product(BaseModel):
    name: str
    price: str
    link: str


class CitilinkParser:

    BASE_URL = "https://www.citilink.ru"

    async def start(self):
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=False)
        context = await self.browser.new_context()
        self.page = await context.new_page()

    async def load_page(self, url):
        await self.page.goto(url)
        await self.page.wait_for_selector(
            '[data-meta-name="SnippetProductHorizontalLayout"]',
            timeout=15_000
        )

    async def parce_products(self) -> list[Product]:
        products = []
        cards = await self.page.query_selector_all(
            '[data-meta-name="SnippetProductHorizontalLayout"]',
        )
        print(f"Найдено товаров: {len(cards)}")

        for card in cards:
            name_el = await card.query_selector(
                '[data-meta-name="Snippet__title"]')       
            name = await name_el.inner_text()

            link_el = await card.query_selector('a[href*="/product/"]')
            href = await link_el.get_attribute("href")
            link = self.BASE_URL + href

            price_el = await card.query_selector(
                "[data-meta-price]"
            )
            price = await price_el.get_attribute("data-meta-price")

            products.append(
                Product(
                    name=name,
                    link=link,
                    price=price
                )
            )

        return products


@app.get("/parser")
async def parser(background_task: BackgroundTasks):
    citi_parser = CitilinkParser()

    async def func(x):
        await citi_parser.start()
        await citi_parser.load_page(x)
        products = await citi_parser.parce_products()
        print(products)

    async def paginator(url, max_pages):
        for page in range(max_pages):
            new_url = url + f"?p={page+1}"
            await func(new_url)

    category_url = "https://www.citilink.ru/catalog/smartfony/"
    background_task.add_task(paginator, category_url, max_pages=5)
    return {
        "message": "Парсер запущен в фоне"
    }
