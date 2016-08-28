\timing
create table q1_data as 
(select
	l_returnflag,
	l_linestatus,
	l_quantity,
	l_extendedprice base_price,
	l_extendedprice * (1 - l_discount) disc_price,
	l_extendedprice * (1 - l_discount) * (1 + l_tax) charge,
	l_extendedprice,
	l_discount
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '115 day'
)
	;
