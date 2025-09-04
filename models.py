from pydantic import BaseModel
from typing import List, Optional, Union
from datetime import datetime


class Restaurant(BaseModel):
    res_name: str
    address: str
    contact_information: str
    restID: str


class Customer(BaseModel):
    name: Optional[str] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    gstin: Optional[str] = None


class PartPayment(BaseModel):
    payment_type: str
    amount: float
    custome_payment_type: Optional[str] = None


class Order(BaseModel):
    orderID: int
    customer_invoice_id: str
    delivery_charges: Union[int, float]
    order_type: str
    payment_type: str
    table_no: Optional[str] = None
    no_of_persons: Optional[int] = None
    discount_total: Union[int, float]
    tax_total: Union[int, float]
    round_off: Union[str, float, int]
    core_total: Union[int, float]
    total: Union[int, float]
    created_on: datetime
    order_from: str
    order_from_id: Optional[str] = None
    sub_order_type: Optional[str] = None
    packaging_charge: Union[int, float]
    status: str
    comment: Optional[str] = None
    service_charge: Union[int, float]
    biller: Optional[str] = None
    assignee: Optional[str] = None
    part_payments: Optional[List[PartPayment]] = []   # ✅ keep default empty list


class Addon(BaseModel):
    group_name: Optional[str] = None
    name: str
    price: Union[int, float]
    quantity: Union[str, int]
    sap_code: Optional[str] = None
    addon_id: Optional[str] = None
    addon_group_id: Optional[str] = None


class OrderItem(BaseModel):
    name: str
    itemid: int
    itemcode: str
    vendoritemcode: Optional[str] = None
    specialnotes: Optional[str] = None
    price: Union[int, float]
    quantity: int
    total: Union[int, float]
    addon: Optional[List[Addon]] = []   # ✅ keep default empty list
    category_name: Optional[str] = None
    sap_code: Optional[str] = None
    discount: Union[int, float]
    tax: Union[int, float]


class Tax(BaseModel):
    title: str
    rate: Union[int, float]
    amount: Union[int, float]


class Discount(BaseModel):
    title: str
    type: str
    rate: Union[int, float]
    amount: Union[int, float]


class Properties(BaseModel):
    Restaurant: Restaurant
    Customer: Customer
    Order: Order
    Tax: Optional[List[Tax]]        # ✅ default empty list
    Discount: Optional[List[Discount]]   # ✅ default empty list
    OrderItem: List[OrderItem]      # ✅ default empty list


class WebhookPayload(BaseModel):
    token: Optional[str] = None
    properties: Properties
    event: str
