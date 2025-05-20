import os

from pydantic import BaseModel, Field, AliasChoices
from dotenv import load_dotenv


load_dotenv()
YELO_API_KEY = os.getenv("YELO_API_KEY", "default_api_key")


class YeloResponses(BaseModel):
    message: str
    status: int


class PostUserYelo(BaseModel):
    api_key: str = YELO_API_KEY
    first_name: str
    last_name: str
    email: str
    phone_no: str
    password: str


class DataUser(BaseModel):
    customer_id: int = Field(validation_alias=AliasChoices("customer_id", "vendor_id"))


class ResponsePostUserYelo(YeloResponses):
    data: DataUser


class PostUserAddressYelo(BaseModel):
    api_key: str = YELO_API_KEY
    customer_id: int
    address: str
    house_no: str
    email: str | None
    phone_no: str | None
    latitude: float
    longitude: float
    name: str  # Full user name
    loc_type: int  # 0 home, 1 work , 2 other. Other can hold up to 10 addresses


class DataAddress(BaseModel):
    id: int


class ResponsePostAddressYelo(YeloResponses):
    data: DataAddress


class GetUserYelo(BaseModel):
    api_key: str
    customer_id: int


class RawCaliddaUser(BaseModel):
    num_interlocutor: int
    num_document: int
    apellidos_nombres: str
    saldo_disponible: float
    fijo: str
    celular: str
    correo: str
    cuenta_contrato: int
    direccion: str
    distrito: str
    NSE: int
    latitud: float
    longitud: float


class CleanAddress(BaseModel):
    address: str
    latitude: float
    longitude: float
    house_no: str
    loc_type: int
    postal_code: str
    id: int | None = None
    upload_status: str | None = None
    error_message: str | None = None


class CleanCustomField(BaseModel):
    app_side: int
    data: str
    input: str
    data_type: str
    display_name: str
    label: str
    required: int
    value: str
    template_id: str
    is_new: bool
    id: str | None = None
    upload_status: str | None = None


class CleanUserData(BaseModel):
    password: str  # document_id
    first_name: str
    last_name: str
    email: str | None = None
    phone_no: str | None = None
    addresses: list[CleanAddress]
    custom_fields: list[CleanCustomField] | None = None
    upload_status: str | None = None
    customer_id: int | None
    error_message: str | None = None
