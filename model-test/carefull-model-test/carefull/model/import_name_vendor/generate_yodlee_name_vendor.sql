create table category_ratios as
select transaction_category_name, count(*) total, count(primary_merchant_name) / count(*)::float vendor_ratio, count(primary_merchant_name) with_vendor, avg(t.obfuscation_rate) obfuscation_rate
from transactions t
group by transaction_category_name
order by 2;

create table description_vendor as
select t.TRANSACTION_CATEGORY_NAME, description, primary_merchant_name, obfuscation_rate, count(*) total
from transactions t, users u
where t.unique_mem_id = u.unique_mem_id
and t.primary_merchant_name is not null
and t.description is not null
and ((u.user_type = 'R') or (u.user_type = 'S' and u.user_group < '95'))
and t.obfuscation_rate >= 0.6
and (t.transaction_category_name not in ('Transfers', 'Salary/Regular Income', 'Sales/Services Income', 'ATM/Cash Withdrawals', 'Credit Card Payments', 'Check Payment','Savings', 'Interest Income', 'Check Payment', 'Other Income', 'Postage/Shipping'))
and (t.transaction_category_name not in ('Mortgage', 'Investment/Retirement Income', 'Refunds/Adjustments', 'Expense Reimbursement') or t.obfuscation_rate >= 0.8)
and (t.transaction_category_name not in ('Deposit', 'Securities Trades', 'Loans') or t.obfuscation_rate >= 0.7)
and (t.transaction_category_name != 'Service Charges/Fees' or (t.primary_merchant_name != 'Past' and t.obfuscation_rate >= 0.8))
group by t.TRANSACTION_CATEGORY_NAME, description, primary_merchant_name, obfuscation_rate
order by DESCRIPTION, PRIMARY_MERCHANT_NAME;

create table by_vendor as
select primary_merchant_name, sum(total) nbr_transations, avg(obfuscation_rate) obfuscation_rate, count(*) nbr_desc,  count(*) /  sum(total) unique_rate
from description_vendor
group by primary_merchant_name;

select dv.description, dv.primary_merchant_name
from description_vendor dv, by_vendor bv
where dv.primary_merchant_name = bv.primary_merchant_name
and bv.nbr_transations > 1
and (bv.nbr_transations < 30 or bv.unique_rate <= 0.7)
order by dv.primary_merchant_name, dv.description;
