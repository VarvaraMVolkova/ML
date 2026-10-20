The model is built on the Analytical Workspace platform.

The task is to build a predictive model to evaluate potential bike buyers in the future.

KPMG dataset - data on customers who own bicycles.
CustomerDemographic - a list of clients and their details
CustomerAddress - customer addresses
Transactions - transactions
NewCustomerList - a list of clients to run through after building the model and training

The sequence of work:
1. Uploading data
2. Visual familiarization with the data.
3. Control and quality of cell filling

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def after_all(df, spark, app):
"""
The function that will be executed after all actions with the dataset
"""

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
	

4. Assembling the schema and setting up relationships between individual tables
The source with Transactions is not completely needed. It is calculated separately and participates in an aggregated form in the logical scheme.select 
    customer_id,
    round(sum(list_price), 2) as summa
from transactions
where order_status = 'Approved'
group by customer_id

For the final dataset, you need to make changes for the fields "Age", "Zip code" and "property_valuation"
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

5. Conduct model training 

6. Prepare a new dataset, which will be transmitted to the model for making a forecast. The condition of the system is that the sequence of fields must be the same.
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

7. Run a model to calculate summa_predicted for a list of new clients

## Results and conclusions
Training sample: 4000 clients, split [80/20].

Forecast for new clients. For the NewCustomerList list, the model outputs `summa_predicted` in a narrow range of 6045.09 - 7065.59 (about 6,000 for most), which is close to the average value of `summa` in the training data (6178.75).

- The narrow spread suggests that the signs poorly explain the differences between clients: the model mainly reproduces the average value.
- Reasons: a limited set of features, noise in the target variable (the amount depends on the number and composition of orders), a small sample (4000 rows).

**Restrictions.** The gaps in the initial data were filled in with the average and fashion; abnormal dates of birth were corrected by heuristics; the model was trained on data from a single period.

**What to improve.**
1. Add attributes from transactions (order frequency, prescription of last purchase, product categories),
2. Compare several algorithms and select parameters for cross-validation.
3. Predict not the amount, but the probability of purchase (with the predicted purchase amount) or the customer segment, as this is often more useful for business.
