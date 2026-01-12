from sqlalchemy import text
from ETL_pipeline import connect_db

INDEXES = [
    """
    CREATE INDEX idx_purchases_vendor_brand_price
    ON purchases (VendorNumber, Brand, PurchasePrice)
    """,
    """
    CREATE INDEX idx_sales_vendor_brand
    ON sales (VendorNo, Brand)
    """,
    """
    CREATE INDEX idx_vendor_invoice_vendor_brand
    ON vendor_invoice (VendorNumber, Freight)
    """,
    """
    CREATE INDEX idx_purchase_prices_brand
    ON purchase_prices (Brand)
    """
]

engine = connect_db()

def create_indexes():
    with engine.begin() as conn:
        for sql in INDEXES:
            conn.execute(text(sql))
            print("Indexes 1 created")
            
if __name__ == "__main__":
    create_indexes()
    print("Indexes created successfully")