from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import auth, contractors, admin

app = FastAPI(
    title="SRPC Loyalty API",
    description="Saraswati Hardware Contractor Loyalty Program",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router,        prefix="/auth",        tags=["Authentication"])
app.include_router(contractors.router, prefix="/contractors", tags=["Contractors"])
app.include_router(admin.router,       prefix="/admin",       tags=["Admin"])

@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "app": "SRPC Loyalty API",
        "version": "1.0.0"
    }
