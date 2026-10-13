"""
Chapter 4: Daft reading a Hudi Copy-on-Write table.

Daft 0.7.26 reads Copy-on-Write tables written by Hudi 0.12 to 0.15 (table version 5 or 6). It
cannot read tables written by Hudi 1.x, even at table version 6, because it looks for the
hoodie.table.keygenerator.class property, which Hudi 1.x does not write. make_daft_table_hudi015.scala
writes a small store_sales table with Hudi 0.15.0 for this example.
  pip install -r requirements-daft.txt
  python daft_example.py
"""
import daft

df = daft.read_hudi("/tmp/ss_cow_015")
features = (df.where(df["ss_store_sk"] == 7)
              .groupby("ss_customer_sk")
              .agg(daft.col("ss_quantity").sum().alias("total_qty"),
                   daft.col("ss_ticket_number").count().alias("tickets")))
features.show()
