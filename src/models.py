from pydantic import BaseModel


class PostUserYelo(BaseModel):
    api_key: str
    first_name: str
    last_name: str
    email: str
    phone_no: str
    password: str


class CustomerIdBody(BaseModel):
    customer_id: int


class ResponsePostUserYelo(BaseModel):
    message: str
    status: int
    data: CustomerIdBody


class PostUserAddressYelo(BaseModel):
    api_key: str
    customer_id: int
    address: str
    house_no: str
    email: str | None
    phone_no: str | None
    latitude: float
    longitude: float
    name: str  # Full user name
    loc_type: int  # 0 home, 1 work , 2 other. Other can hold up to 10 addresses

    # VALIDAR que, entre el correo y el telefono, al menos uno de los dos sea obligatorio


class AddressIdBody(BaseModel):
    id: int


class ResponsePostAddressYelo(BaseModel):
    message: str
    status: int
    data: AddressIdBody


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
    id: int | None = None
    upload_status: str | None = None


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
