-- construct_array must build a non-null array on both adapters.
-- A passing singular test returns zero rows.
with built as (

    select {{ construct_array(["'x'"]) }} as arr

)

select *
from built
where arr is null
