--=========================================================
--	COMPANY NAME : ROCKY MOUNTAIN CHOCOLATE FACTORY, INC.	--UPDATE HERE
--	CENTRAL INDEX KEY : 1616262								--UPDATE HERE
--=========================================================

drop table temp_cik;
create temporary table temp_cik as
select
	 '1616262' as "cik"		--UPDATE HERE
	, '%balance_sheets_%' as "bs_include"
	, '%balance_sheets_parenthetical%' as "bs_exclude"
	, '%statements_of_earnings_%' as "is_include"
;


drop table temp_sqlitemaster;
create temporary table temp_sqlitemaster as
select
	 type
	, upper( --name
	  	 replace(
		  	 replace(
		  	 	 lower( name )
		  	 	, 'operations'
		  	 	, 'earnings' 
		  	 )
		  	, 'income'
		  	, 'earnings'
		 )
	  ) as "name"
from
	sqlite_master
;


--TEMPLATE TO UNION FOR ALL BALANCE SHEET SUMMARY
select
	 'select * from "' || w1.name || '"' as "bs_query"
--	 group_concat( 'select * from "' || w1.name || '"', ' union all ' ) as "bs_query"
	 	--UPDATE HERE
--	 w1.name
--	, replace( w1.name, rtrim( w1.name, replace( w1.name, '_', '' ) ), '' ) as "filing_number"
--	, w2.company_name
--	, w2.cik
--	, group_concat( 'select date, total_assets, total_current_assets, property_and_equipment_net, inventories, accounts_receivable_net_and_other, inventories, total_liabilities_and_stockholders_equity - total_stockholders_equity as "total_liabilities", total_current_liabilities, accounts_payable, long_term_debt, total_stockholders_equity, retained_earnings from "' || w1.name || '"', ' union all ' ) as "query"
from
	 temp_sqlitemaster 	w1
left join
	filing_list			w2 on
		 replace( w1.name, rtrim( w1.name, replace( w1.name, '_', '' ) ), '' )
		 =
		 w2.filing_number
where
	 w1.type = 'table'
and
	 w1.name like '10_K%'
and
	 w2.cik = ( select cik from temp_cik )
and
	 lower( w1.name ) like ( select bs_include from temp_cik )
and
	 lower( w1.name ) not like ( select bs_exclude from temp_cik )
--group by
--	 1,2,3,4
;



--TEMPLATE TO UNION FOR ALL INCOME STATEMENT SUMMARY

--select * from "10_K2017_02_10_Consolidated_Statements_Of_Operations_2251317588807"

select
	 'select * from "' || w1.name || '"' as "is_query"
--	 group_concat( 'select * from "' || w1.name || '"', ' union all ' ) as "is_query"
	 	--UPDATE HERE
--	 w1.name
--	, replace( w1.name, rtrim( w1.name, replace( w1.name, '_', '' ) ), '' ) as "filing_number"
--	, w2.company_name
--	, w2.cik
--	, group_concat( 'select date, total_assets, total_current_assets, property_and_equipment_net, inventories, accounts_receivable_net_and_other, inventories, total_liabilities_and_stockholders_equity - total_stockholders_equity as "total_liabilities", total_current_liabilities, accounts_payable, long_term_debt, total_stockholders_equity, retained_earnings from "' || w1.name || '"', ' union all ' ) as "query"
from
	 temp_sqlitemaster 	w1
left join
	filing_list			w2 on
		 replace( w1.name, rtrim( w1.name, replace( w1.name, '_', '' ) ), '' )
		 =
		 w2.filing_number
where
	 w1.type = 'table'
and
	 w1.name like '10_K%'
and
	 w2.cik = ( select cik from temp_cik )
and
	 lower( w1.name ) like ( select is_include from temp_cik )
--and
--	 lower( w1.name ) not like ( select exclude from temp_cik )
--group by
--	 1,2,3,4
;


--TO VIEW COMPILED BALANCE SHEET
drop table temp_balance_sheet;
create temporary table temp_balance_sheet as
select
	 ( select cik from temp_cik ) as "cik"
	, e1.*
from(
	--INSERT QUERY HERE
	select
		 w1.date
		, cast( w1.cash_and_cash_equivalents as float ) / 1000000 as "cash_and_equivalents"
		, cast( w1.accounts_receivable_less_allowance_for_doubtful_accounts_of_870735_and_1341853_respectively as float ) / 1000000 as "accounts_receivable_net"
		, cast( w1.inventories as float ) / 1000000 as "inventories"
		, cast( w1.total_current_assets as float ) / 1000000 as "total_current_assets"
		, ( coalesce( cast( w1.property_and_equipment_net as float ), 0 )
		  + coalesce( cast( w1.lease_right_of_use_asset as float ), 0 ) ) / 1000000
		  as "property_plant_and_equipment_net"
		, cast( w1.goodwill_net as float ) / 1000000 as "goodwill"
		, ( coalesce( cast( w1.intangible_assets_net as float ), 0 )
		  + coalesce( cast( w1.franchise_rights_net as float ), 0 ) ) / 1000000
		  as "intangible_assets"
		, cast( w1.total_assets as float ) / 1000000 as "total_assets"
		, cast( w1.lease_liability as float ) / 1000000 as "short_term_debt"
		, null / 1000000 as "short_term_unearned_revenue"
		, cast( w1.accounts_payable as float ) / 1000000 as "accounts_payable"
		, cast( w1.total_current_liabilities as float ) / 1000000 as "total_current_liabilities"
		, null / 1000000 as "long_term_unearned_revenue"
		, cast( w1.lease_liability_less_current_portion as float ) / 1000000 as "long_term_debt"
		, ( coalesce( cast( w1.lease_liability as float ), 0 )
		  + coalesce( cast( w1.lease_liability_less_current_portion as float ), 0 ) ) / 1000000
		  as "total_debt"
		, ( coalesce( cast( w1.total_current_liabilities as float ), 0 )
		  + coalesce( cast( w1.lease_liability_less_current_portion as float ), 0 )
		  + coalesce( cast( contract_liabilities_less_current_portion as float ), 0 ) ) / 1000000
		  as "total_liabilities"
		, ( coalesce( cast( w1.common_stock_001_par_value_46000000_shares_authorized_6186356_shares_and_6074293_shares_issued_and_outstanding_respectively as float ), 0 )
		  + coalesce( cast( w1.additional_paid_in_capital as float ), 0 ) ) / 1000000
		  as "total_paid_in_capital"
		, null / 1000000 as "treasury_stock"
		, cast( w1.retained_earnings as float ) / 1000000 as "retained_earnings"
		, cast( w1.total_stockholders_equity as float ) / 1000000 as "total_shareholder_equity"
		, null as "non_controlling_interest"
		, cast( w1.total_liabilities_and_stockholders_equity as float ) / 1000000 as "total_liabilities_and_equity"
	from
		"10_K2022_05_27_CONSOLIDATED_BALANCE_SHEETS_13686522976723" w1
		union all
	select
		 w1.date
		, cast( w1.cash_and_cash_equivalents as float ) / 1000000 as "cash_and_equivalents"
		, cast( w1.accounts_receivable_less_allowance_for_doubtful_accounts_of_638907_and_489502_respectively as float ) / 1000000 as "accounts_receivable_net"
		, cast( w1.inventories as float ) / 1000000 as "inventories"
		, cast( w1.total_current_assets as float ) / 1000000 as "total_current_assets"
		, ( coalesce( cast( w1.property_and_equipment_net as float ), 0 )
		  + coalesce( cast( w1.lease_right_of_use_asset as float ), 0 ) ) / 1000000
		  as "property_plant_and_equipment_net"
		, cast( w1.goodwill_net as float ) / 1000000 as "goodwill"
		, ( coalesce( cast( w1.intangible_assets_net as float ), 0 )
		  + coalesce( cast( w1.franchise_rights_net as float ), 0 ) ) / 1000000
		  as "intangible_assets"
		, cast( w1.total_assets as float ) / 1000000 as "total_assets"
		, ( coalesce( cast( w1.current_maturities_of_long_term_debt as float ), 0 )
		  + coalesce( cast( w1.lease_liability as float ), 0 ) ) / 1000000
		  as "short_term_debt"
		, null / 1000000 as "short_term_unearned_revenue"
		, cast( w1.accounts_payable as float ) / 1000000 as "accounts_payable"
		, cast( w1.total_current_liabilities as float ) / 1000000 as "total_current_liabilities"
		, null / 1000000 as "long_term_unearned_revenue"
		, cast( w1.lease_liability_less_current_portion as float ) / 1000000 as "long_term_debt"
		, ( coalesce( cast( w1.current_maturities_of_long_term_debt as float ), 0 )
		  + coalesce( cast( w1.lease_liability as float ), 0 )
		  + coalesce( cast( w1.lease_liability_less_current_portion as float ), 0 ) ) / 1000000
		  as "total_debt"
		, ( coalesce( cast( w1.total_current_liabilities as float ), 0 )
		  + coalesce( cast( w1.lease_liability_less_current_portion as float ), 0 )
		  + coalesce( cast( contract_liabilities_less_current_portion as float ), 0 ) ) / 1000000
		  as "total_liabilities"
		, ( coalesce( cast( w1.common_stock_001_par_value_46000000_shares_authorized_6019532_shares_and_5957827_shares_issued_and_outstanding_respectively as float ), 0 )
		  + coalesce( cast( w1.additional_paid_in_capital as float ), 0 ) ) / 1000000
		  as "total_paid_in_capital"
		, null / 1000000 as "treasury_stock"
		, cast( w1.retained_earnings as float ) / 1000000 as "retained_earnings"
		, cast( w1.total_stockholders_equity as float ) / 1000000 as "total_shareholder_equity"
		, null as "non_controlling_interest"
		, cast( w1.total_liabilities_and_stockholders_equity as float ) / 1000000 as "total_liabilities_and_equity"
	from
		"10_K2020_05_29_CONSOLIDATED_BALANCE_SHEETS_13686520925557" w1
		union all
	select
		 w1.date
		, cast( w1.cash_and_cash_equivalents as float ) / 1000000 as "cash_and_equivalents"
		, cast( w1.accounts_receivable_less_allowance_for_doubtful_accounts_of_479472_and_487446_respectively as float ) / 1000000 as "accounts_receivable_net"
		, cast( w1.inventories_less_reserve_for_slow_moving_inventory_of_357706_and_249051_respectively as float ) / 1000000 as "inventories"
		, cast( w1.total_current_assets as float ) / 1000000 as "total_current_assets"
		, cast( w1.property_and_equipment_net as float ) / 1000000 as "property_plant_and_equipment_net"
		, cast( w1.goodwill_net as float ) / 1000000 as "goodwill"
		, ( coalesce( cast( w1.intangible_assets_net as float ), 0 )
		  + coalesce( cast( w1.franchise_rights_net as float ), 0 ) ) / 1000000
		  as "intangible_assets"
		, cast( w1.total_assets as float ) / 1000000 as "total_assets"
		, cast( w1.current_maturities_of_long_term_debt as float ) / 1000000 as "short_term_debt"
		, cast( w1.deferred_income as float ) / 1000000 as "short_term_unearned_revenue"
		, cast( w1.accounts_payable as float ) / 1000000 as "accounts_payable"
		, cast( w1.total_current_liabilities as float ) / 1000000 as "total_current_liabilities"
		, null / 1000000 as "long_term_unearned_revenue"
		, cast( w1.long_term_debt_less_current_maturities as float ) / 1000000 as "long_term_debt"
		, ( coalesce( cast( w1.current_maturities_of_long_term_debt as float ), 0 )
		  + coalesce( cast( w1.long_term_debt_less_current_maturities as float ), 0 ) ) / 1000000
		  as "total_debt"
		, ( coalesce( cast( w1.total_current_liabilities as float ), 0 )
		  + coalesce( cast( w1.long_term_debt_less_current_maturities as float ), 0 ) ) / 1000000
		  as "total_liabilities"
		, ( coalesce( cast( w1.common_stock_001_par_value_per_share_46000000_shares_authorized_5903436_and_5854372_issued_and_5903436_and_5854372_outstanding_respectively as float ), 0 )
		  + coalesce( cast( w1.additional_paid_in_capital as float ), 0 ) ) / 1000000
		  as "total_paid_in_capital"
		, null / 1000000 as "treasury_stock"
		, cast( w1.retained_earnings as float ) / 1000000 as "retained_earnings"
		, cast( w1.total_stockholders_equity as float ) / 1000000 as "total_shareholder_equity"
		, null as "non_controlling_interest"
		, cast( w1.total_liabilities_and_stockholders_equity as float ) / 1000000 as "total_liabilities_and_equity"
	from
		"10_K2018_05_15_CONSOLIDATED_BALANCE_SHEETS_13686518837395" w1
) e1
order by
	 e1.date
;


--TO VIEW COMPILED INCOME STATEMENT
drop table temp_income_statement;
create temporary table temp_income_statement as
select
	 ( select cik from temp_cik ) as "cik"
	, e1.*
from(
	--INSERT QUERY HERE
	select
		 w1.date
		, w2.common_stock_outstanding
		, cast( w1.total_revenue as float ) / 1000000 as "total_sales"
		, - cast( w1.cost_of_sales as float ) / 1000000 as "total_cost_of_sales"
		, ( - coalesce( cast( w1.sales_and_marketing as float ), 0 )
		  - coalesce( cast( w1.general_and_administrative as float ), 0 ) ) / 1000000
		  as "selling_general_and_administrative_expense"
		, null / 1000000 as "research_and_development"
		, - cast( w1.depreciation_and_amortization_exclusive_of_depreciation_and_amortization_expense_of_620798_625526_and_597430_respectively_included_in_cost_of_sales as float ) / 1000000 as "depreciation_amortisation"
		, null / 1000000 as "capex"
		, cast( w1.income_loss_from_operations as float ) / 1000000 as "operating_income"
		, cast( w1.interest_expense as float ) / 1000000 as "interest_expense"
		, - cast( w1.income_tax_provision as float ) / 1000000 as "income_tax"
		, cast( w1.consolidated_net_income_loss as float ) / 1000000 as "continuing_net_earnings_total"
		, null / 1000000 as "discontinued_net_earnings_total"
		, cast( w1.consolidated_net_income_loss as float ) / 1000000 as "net_earnings_to_shareholder"
	from
		"10_K2022_05_27_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13686522976723" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( cast( w1.common_stock_shares_outstanding_in_shares as float ) / 1000000, 2 ) as "common_stock_outstanding"
		from
			"10_K2022_05_27_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICALS_13686522976723" w1
		where
			 w1.common_stock_shares_outstanding_in_shares is not null
		order by
			 w1.date desc
		limit 3
	)																			w2 on
			 w1.date = w2.end_of_month
	where
		 w1.date in(
		 	select
		 		distinct
		 		 a1.date
		 	from
		 		"10_K2022_05_27_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13686522976723" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, cast( w1.total_revenues as float ) / 1000000 as "total_sales"
		, - cast( w1.cost_of_sales as float ) / 1000000 as "total_cost_of_sales"
		, ( - coalesce( cast( w1.sales_and_marketing as float ), 0 )
		  - coalesce( cast( w1.general_and_administrative as float ), 0 ) ) / 1000000
		  as "selling_general_and_administrative_expense"
		, null / 1000000 as "research_and_development"
		, - cast( w1.depreciation_and_amortization_exclusive_of_depreciation_and_amortization_expense_of_597430_555926_and_523034_respectively_included_in_cost_of_sales as float ) / 1000000 as "depreciation_amortisation"
		, null / 1000000 as "capex"
		, cast( w1.income_from_operations as float ) / 1000000 as "operating_income"
		, cast( w1.interest_expense as float ) / 1000000 as "interest_expense"
		, - cast( w1.income_tax_provision as float ) / 1000000 as "income_tax"
		, cast( w1.consolidated_net_income as float ) / 1000000 as "continuing_net_earnings_total"
		, null / 1000000 as "discontinued_net_earnings_total"
		, cast( w1.consolidated_net_income as float ) / 1000000 as "net_earnings_to_shareholder"
	from
		"10_K2020_05_29_CONSOLIDATED_STATEMENTS_OF_INCOME_13686520925557" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( cast( w1.common_stock_shares_outstanding_in_shares as float ) / 1000000, 2 ) as "common_stock_outstanding"
		from
			"10_K2020_05_29_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICALS_13686520925557" w1
		where
			 w1.common_stock_shares_outstanding_in_shares is not null
		order by
			 w1.date desc
		limit 3
	)																			w2 on
			 w1.date = w2.end_of_month
	where
		 w1.date in(
		 	select
		 		distinct
		 		 a1.date
		 	from
		 		"10_K2020_05_29_CONSOLIDATED_STATEMENTS_OF_INCOME_13686520925557" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, cast( w1.total_revenues as float ) / 1000000 as "total_sales"
		, - cast( w1.cost_of_sales as float ) / 1000000 as "total_cost_of_sales"
		, ( - coalesce( cast( w1.sales_marketing as float ), 0 )
		  - coalesce( cast( w1.general_and_administrative as float ), 0 ) ) / 1000000
		  as "selling_general_and_administrative_expense"
		, null / 1000000 as "research_and_development"
		, - cast( w1.depreciation_and_amortization_exclusive_of_depreciation_and_amortization_expense_of_523034_447651_and_404391_respectively_included_in_cost_of_sales as float ) / 1000000 as "depreciation_amortisation"
		, null / 1000000 as "capex"
		, cast( w1.operating_income as float ) / 1000000 as "operating_income"
		, cast( w1.interest_expense as float ) / 1000000 as "interest_expense"
		, - cast( w1.income_tax_expense_benefit as float ) / 1000000 as "income_tax"
		, cast( w1.net_income as float ) / 1000000 as "continuing_net_earnings_total"
		, null / 1000000 as "discontinued_net_earnings_total"
		, cast( w1.net_income_attributable_to_rmcf_stockholders as float ) / 1000000 as "net_earnings_to_shareholder"
	from
		"10_K2018_05_15_CONSOLIDATED_STATEMENTS_OF_INCOME_13686518837395" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( cast( w1.common_stock_shares_outstanding_in_shares as float ) / 1000000, 2 ) as "common_stock_outstanding"
		from
			"10_K2018_05_15_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICALS_13686518837395" w1
		where
			 w1.common_stock_shares_outstanding_in_shares is not null
		order by
			 w1.date desc
		limit 3
	)																			w2 on
			 w1.date = w2.end_of_month
	where
		 w1.date in(
		 	select
		 		distinct
		 		 a1.date
		 	from
		 		"10_K2018_05_15_CONSOLIDATED_STATEMENTS_OF_INCOME_13686518837395" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
) e1
order by
	 e1.date
;



--=======================================
--	CALL OUT FINANCIAL STATEMENTS
--=======================================

with
	temp_is as(
		select
			 w1.*
		from
			temp_income_statement w1
	)
	,
	temp_bs as(
		select
			 w1.*
		from
			temp_balance_sheet w1
	)
select
	 w1.cik
	, w1.date
	, w1.common_stock_outstanding
	, w1.total_sales
	, w1.total_sales + w1.total_cost_of_sales as "gp"
	, w1.selling_general_and_administrative_expense as "sga"
	, round( cast( ( w1.total_sales + w1.total_cost_of_sales ) as float ) / w1.total_sales * 100, 2 ) as "gpm_pct"
	, round( cast( w1.selling_general_and_administrative_expense as float ) / w1.total_sales * 100, 2 ) as "sga_pct"
	, round( cast( w1.operating_income as float ) / w1.total_sales * 100, 2 ) as "op_earnings_margin_pct"
	, round( cast( w1.total_sales as float ) / w1.common_stock_outstanding, 2 ) as "sales_per_share"
	, round( cast( w1.total_sales + w1.total_cost_of_sales as float ) / w1.common_stock_outstanding, 2 ) as "gp_per_share"
	, round( cast( w1.selling_general_and_administrative_expense as float ) / w1.common_stock_outstanding, 2 ) as "sga_per_share"
	, round( cast( w1.net_earnings_to_shareholder as float ) / w1.common_stock_outstanding, 2 ) as "earnings_per_share"
	, round( cast( w2.total_shareholder_equity as float ) / w1.common_stock_outstanding, 2 ) as "equity_per_share"
	, round( cast( w2.total_debt as float ) / w1.common_stock_outstanding, 2 ) as "debt_per_share"
	, round( cast( w2.accounts_receivable_net as float ) / w1.common_stock_outstanding, 2 ) as "ar_per_share"
	, round( cast( w2.inventories as float ) / w1.common_stock_outstanding, 2 ) as "invntry_per_share"
	, round( cast( w2.accounts_payable as float ) / w1.common_stock_outstanding, 2 ) as "ap_per_share"
	, round( cast( w2.accounts_receivable_net as float ) / w1.total_sales * 365, 2 ) as "dso"
	, round( cast( w2.inventories as float ) / w1.total_sales * 365, 2 ) as "dsi"
	, round( cast( w2.accounts_payable as float ) / w1.total_sales * 365, 2 ) as "dsp"
from
	temp_is w1
left join
	temp_bs w2 on
		 w1.date = w2.date
order by
	 w1.date
;
select
	 w1.*
from
	temp_bs w1
;





--=========================
--	DROP TEMP TABLES
--=========================

drop table temp_cik;
drop table temp_sqlitemaster;
drop table temp_balance_sheet;
drop table temp_income_statement;
