from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def after_all(df, spark, app): """ The function that will be executed after all actions with the dataset """
norm_job_title_udf = udf(lambda jt: norm_job_title(jt), StringType())
df = df.withColumn('job_title', norm_job_title_udf(df.job_title))
return df

def norm_job_title(job_title):
"""
Returns the normalized representation of
job_title """
if not job_title:
return job_title
toks = job_title.rsplit(' ', 1)
if toks[-1] in ('I', 'II', 'III', 'IV'):
return toks[0]
    return ' '.join(toks)
	
"""
step 4  
"""
select 
    customer_id,
    round(sum(list_price), 2) as summa
from transactions
where order_status = 'Approved'
group by customer_id

"""
 dataset prepare with right columns order
"""
select
    gender,
    case 
        when dob is null then round(mean_age, 0)
        when age > 100 then age - 100
        else age
    end as age,
    coalesce(past_3_years_bike_related_purchases, mean_past_3_years_bike_related_purchases) as past_3_years_bike_related_purchases,
    job_title,
    job_industry_category,
    wealth_segment,
    deceased_indicator,
    owns_car,
    coalesce(tenure, 0) as tenure,
    case 
        when postcode is null then mode_postcode
        else left(postcode, 2)
    end as postcode,
    state,
    coalesce(property_valuation, mode_property_valuation) as property_valuation,
    summa
from (
    with mean_values as (
        select 
            round(mean(age), 0) as mean_age,
            round(mean(past_3_years_bike_related_purchases), 0) as mean_past_3_years_bike_related_purchases,
            mode(left(postcode, 2)) as mode_postcode,
            mode(property_valuation) as mode_property_valuation
        from child
    )
    select * from child cross join mean_values
)


"""
 prepare dataset for calculate forecast 
"""
select
    gender,
    floor(months_between(current_date(), dob) / 12) as age,
    past_3_years_bike_related_purchases,
    tenure,
    property_valuation,
    job_title,
    job_industry_category,
    wealth_segment,
    deceased_indicator,
    owns_car,
    left(postcode, 2) as postcode,
    state
from child
