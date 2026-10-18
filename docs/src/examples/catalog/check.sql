select * from products order by id;
select * from product_dimensions order by product_id;
select * from product_tags order by product_id, position;
select * from variants order by product_id, position;
select * from variant_options order by product_id, variant_position, position;
