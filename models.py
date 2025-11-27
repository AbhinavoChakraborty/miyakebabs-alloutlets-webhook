from __future__ import annotations

from pydantic import BaseModel
from typing import List, Optional, Union
from datetime import datetime


class Restaurant(BaseModel):
    res_name: Optional[str] = None
    address: Optional[str] = None
    contact_information: Optional[str] = None
    restID: Optional[str] = None

    model_config = {"extra": "ignore"}


class Customer(BaseModel):
    name: Optional[str] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    gstin: Optional[str] = None

    model_config = {"extra": "ignore"}


class PartPayment(BaseModel):
    payment_type: Optional[str] = None
    amount: Optional[Union[int, float]] = None
    custome_payment_type: Optional[str] = None

    model_config = {"extra": "ignore"}


class Order(BaseModel):
    orderID: Optional[Union[int, str]] = None
    customer_invoice_id: Optional[str] = None
    delivery_charges: Optional[Union[int, float]] = None
    order_type: Optional[str] = None
    payment_type: Optional[str] = None
    table_no: Optional[Union[str, int]] = None
    no_of_persons: Optional[Union[int, str]] = None
    discount_total: Optional[Union[int, float]] = None
    tax_total: Optional[Union[int, float]] = None
    round_off: Optional[Union[str, float, int]] = None
    core_total: Optional[Union[int, float]] = None
    total: Optional[Union[int, float]] = None
    created_on: Optional[datetime] = None
    order_from: Optional[str] = None
    order_from_id: Optional[Union[str, int]] = None
    sub_order_type: Optional[str] = None
    packaging_charge: Optional[Union[int, float]] = None
    status: Optional[str] = None
    comment: Optional[str] = None
    service_charge: Optional[Union[int, float]] = None
    biller: Optional[str] = None
    assignee: Optional[str] = None

    part_payments: List[PartPayment] = []

    model_config = {"extra": "ignore"}


class Addon(BaseModel):
    group_name: Optional[str] = None
    name: Optional[str] = None
    price: Optional[Union[int, float]] = None
    quantity: Optional[Union[str, int]] = None
    sap_code: Optional[str] = None

    addon_id: Optional[Union[str, int]] = None
    addon_group_id: Optional[Union[str, int]] = None  

    model_config = {"extra": "ignore"}



class OrderItem(BaseModel):
    name: Optional[str] = None
    itemid: Optional[Union[int, str]] = None
    itemcode: Optional[str] = None
    vendoritemcode: Optional[str] = None
    specialnotes: Optional[str] = None
    price: Optional[Union[int, float]] = None
    quantity: Optional[Union[int, str]] = None
    total: Optional[Union[int, float]] = None

    addon: List[Addon] = []

    category_name: Optional[str] = None
    sap_code: Optional[str] = None
    discount: Optional[Union[int, float]] = None
    tax: Optional[Union[int, float]] = None

    model_config = {"extra": "ignore"}


class Tax(BaseModel):
    title: Optional[str] = None
    rate: Optional[Union[int, float]] = None
    amount: Optional[Union[int, float]] = None

    model_config = {"extra": "ignore"}


class Discount(BaseModel):
    title: Optional[str] = None
    type: Optional[str] = None
    rate: Optional[Union[int, float]] = None
    amount: Optional[Union[int, float]] = None

    model_config = {"extra": "ignore"}


class Properties(BaseModel):
    Restaurant: Restaurant
    Customer: Customer
    Order: Order
    Tax: Optional[List[Tax]]         # ✅ default empty list
    Discount: Optional[List[Discount]]    # ✅ default empty list
    OrderItem: List[OrderItem] 

    model_config = {"extra": "ignore"}



class WebhookPayload(BaseModel):
    token: Optional[str] = None
    properties: Optional[Properties] = None
    event: Optional[str] = None

    model_config = {"extra": "ignore"}
