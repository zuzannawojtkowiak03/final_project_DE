import sys
sys.dont_write_bytecode = True

from Transformation.transformation_users import transform_users
from Transformation.transformation_cards import transform_cards
from Transformation.transformation_mcc import transform_mcc
from Transformation.transformation_transactions import transform_transactions

from Curated.dim_customers import dim_customers
from Curated.dim_cards import dim_cards
from Curated.dim_mcc import dim_mcc
from Curated.dim_merchants import dim_merchants
from Curated.dim_date import dim_date
from Curated.fact_transactions import fact_transactions

from Marts.finance_mart import finance_mart
from Marts.customer_mart import customer_mart
from Marts.merchant_mart import merchant_mart


def run_pipeline():
    print("\nStarting pipeline")

    # Transformation
    print("\n# Transformation")
    transform_users()
    transform_cards()
    transform_mcc()
    transform_transactions()
    print("Transformation done")

    # Curated
    print("\n# Curated")
    dim_customers()
    dim_cards()
    dim_mcc()
    dim_merchants()
    dim_date()
    fact_transactions()
    print("Curated done")

    # Marts
    print("\n# Marts")
    finance_mart()
    customer_mart()
    merchant_mart()
    print("Marts done")

    print("\nPipeline finished\n")


if __name__ == "__main__":
    run_pipeline()
