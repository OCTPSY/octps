--============================================
--	COMPANY NAME : NIGHTFOOD HOLDINGS, INC.		--UPDATE HERE
--	CENTRAL INDEX KEY : 1593001					--UPDATE HERE
--============================================

drop table temp_cik;
create temporary table temp_cik as
select
	 '1593001' as "cik"		--UPDATE HERE
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
	  	 	 lower( name )
	  	 	, 'operations'
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
order by
	 1 desc
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
order by
	 1 desc
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
		, cast( w1.cash as float ) / 1000000 as "cash_and_equivalents"
		, cast( w1.accounts_receivable_net_of_allowance_of_0_and_0_respectively as float ) / 1000000 as "accounts_receivable_net"
		, cast( w1.inventories_net_of_allowance_of_24403_and_0_respectively as float ) / 1000000 as "inventories"
		, cast( w1.total_current_assets as float ) / 1000000 as "total_current_assets"
		, null / 1000000 as "property_plant_and_equipment_net"
		, null / 1000000 as "goodwill"
		, null / 1000000 as "intangible_assets"
		, cast( w1.total_assets as float ) / 1000000 as "total_assets"
		, ( coalesce( cast( w1.convertible_notes_payable_net_of_debt_discounts_and_unamortized_beneficial_conversion_feature as float ), 0 )
		  + coalesce( cast( w1.short_term_borrowings_line_of_credit as float ), 0 ) ) / 1000000
		  as "short_term_debt"
		, cast( w1.accounts_payable as float ) / 1000000 as "accounts_payable"
		, cast( w1.total_current_liabilities as float ) / 1000000 as "total_current_liabilities"
		, null / 1000000 as "long_term_debt"
		, ( coalesce( cast( w1.convertible_notes_payable_net_of_debt_discounts_and_unamortized_beneficial_conversion_feature as float ), 0 )
		  + coalesce( cast( w1.short_term_borrowings_line_of_credit as float ), 0 ) ) / 1000000
		  as "total_debt"
		, cast( w1.total_liabilities as float ) / 1000000 as "total_liabilities"
		, ( coalesce( cast( w1.common_stock_0_001_par_value_200000000_shares_authorized_and_80707467_issued_and_outstanding_as_of_june_30_2021_and_61796680_issued_and_outstanding_as_of_june_30_2020_respectively as float ), 0 )
		  + coalesce( cast( w1.additional_paid_in_capital as float ), 0 ) ) / 1000000
		  as "total_paid_in_capital"
		, null / 1000000 as "treasury_stock"
		, cast( w1.accumulated_deficit as float ) / 1000000 as "retained_earnings"
		, cast( w1.total_stockholders_equity_deficit as float ) / 1000000 as "total_shareholder_equity"
		, null / 1000000 as "non_controlling_interest"
		, cast( w1.total_liabilities_and_stockholders_equity_deficit as float ) / 1000000 as "total_liabilities_and_equity"
	from
		"10_K2021_10_13_CONSOLIDATED_BALANCE_SHEETS_55406211321787" w1
		union all
	select
		 w1.date
		, cast( w1.cash as float ) / 1000000 as "cash_and_equivalents"
		, cast( w1.accounts_receivable_net_of_allowance_of_0_and_0_respectively as float ) / 1000000 as "accounts_receivable_net"
		, cast( w1.inventories as float ) / 1000000 as "inventories"
		, cast( w1.total_current_assets as float ) / 1000000 as "total_current_assets"
		, null / 1000000 as "property_plant_and_equipment_net"
		, null / 1000000 as "goodwill"
		, null / 1000000 as "intangible_assets"
		, cast( w1.total_assets as float ) / 1000000 as "total_assets"
		, ( coalesce( cast( w1.convertible_notes_payable_net_of_debt_discounts_and_unamortized_beneficial_conversion_feature as float ), 0 )
		  + coalesce( cast( w1.short_term_borrowings as float ), 0 ) ) / 1000000
		  as "short_term_debt"
		, cast( w1.accounts_payable as float ) / 1000000 as "accounts_payable"
		, cast(
		  	 case when w1.total_current_liabilities = 29955272 then 2955272
		  	 	  else w1.total_current_liabilities
		  	 end
		  as float ) / 1000000 as "total_current_liabilities"
		, null / 1000000 as "long_term_debt"
		, ( coalesce( cast( w1.convertible_notes_payable_net_of_debt_discounts_and_unamortized_beneficial_conversion_feature as float ), 0 )
		  + coalesce( cast( w1.short_term_borrowings as float ), 0 ) ) / 1000000
		  as "total_debt"
		, cast(
		  	 case when w1.total_current_liabilities = 29955272 then 2955272
		  	 	  else w1.total_current_liabilities
		  	 end
		  as float ) / 1000000 as "total_liabilities"
		, ( coalesce( cast( w1.common_stock_0_001_par_value_200000000_shares_authorized_and_53773856_issued_and_outstanding_as_of_june_30_2019_and_42608329_outstanding_as_of_june_30_2018_respectively as float ), 0 )
		  + coalesce( cast( w1.additional_paid_in_capital as float ), 0 ) ) / 1000000
		  as "total_paid_in_capital"
		, null / 1000000 as "treasury_stock"
		, cast( w1.accumulated_deficit as float ) / 1000000 as "retained_earnings"
		, cast( w1.total_stockholders_deficit as float ) / 1000000 as "total_shareholder_equity"
		, null / 1000000 as "non_controlling_interest"
		, cast( w1.total_liabilities_and_stockholders_deficit as float ) / 1000000 as "total_liabilities_and_equity"
	from
		"10_K2019_10_15_CONSOLIDATED_BALANCE_SHEETS_55406191151122" w1
		union all
	select
		 w1.date
		, cast( w1.cash as float ) / 1000000 as "cash_and_equivalents"
		, cast( w1.accounts_receivable_net_of_allowance_of_0_and_22681_respectively as float ) / 1000000 as "accounts_receivable_net"
		, cast( w1.inventories as float ) / 1000000 as "inventories"
		, cast( w1.total_current_assets as float ) / 1000000 as "total_current_assets"
		, null / 1000000 as "property_plant_and_equipment_net"
		, null / 1000000 as "goodwill"
		, null / 1000000 as "intangible_assets"
		, cast( w1.total_assets as float ) / 1000000 as "total_assets"
		, ( coalesce( cast( w1.convertible_notes_payable_net_of_debt_discounts_and_unamortized_beneficial_conversion_feature as float ), 0 )
		  + coalesce( cast( w1.short_term_borrowings as float ), 0 )
		  + coalesce( cast( w1.advance_related_party as float ), 0 )
		  + coalesce( cast( w1.advance_from_shareholders as float ), 0 ) ) / 1000000
		  as "short_term_debt"
		, cast( w1.accounts_payable as float ) / 1000000 as "accounts_payable"
		, cast( w1.total_current_liabilities as float ) / 1000000 as "total_current_liabilities"
		, cast( w1.long_term_borrowings as float ) / 1000000 as "long_term_debt"
		, ( coalesce( cast( w1.convertible_notes_payable_net_of_debt_discounts_and_unamortized_beneficial_conversion_feature as float ), 0 )
		  + coalesce( cast( w1.short_term_borrowings as float ), 0 ) ) / 1000000
		  as "total_debt"
		, ( coalesce( cast( w1.total_current_liabilities as float ), 0 )
		  + coalesce( cast( w1.long_term_borrowings as float ), 0 ) ) / 1000000 as "total_liabilities"
		, ( coalesce( cast( w1.common_stock_0_001_par_value_100000000_shares_authorized_and_29724432_issued_and_outstanding_as_of_june_30_2017_and_28501932_outstanding_as_of_june_30_2016_respectively as float ), 0 )
		  + coalesce( cast( w1.additional_paid_in_capital as float ), 0 ) ) / 1000000
		  as "total_paid_in_capital"
		, null / 1000000 as "treasury_stock"
		, cast( w1.accumulated_deficit as float ) / 1000000 as "retained_earnings"
		, cast( w1.total_stockholders_deficit as float ) / 1000000 as "total_shareholder_equity"
		, null / 1000000 as "non_controlling_interest"
		, cast( w1.total_liabilities_and_stockholders_deficit as float ) / 1000000 as "total_liabilities_and_equity"
	from
		"10_K2017_10_03_CONSOLIDATED_BALANCE_SHEETS_55406171117422" w1
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
		, cast( w1.revenues_net_of_slotting_and_promotion as float ) / 1000000 as "total_sales"
		, - cast( w1.cost_of_product_sold as float ) / 1000000 as "total_cost_of_sales"
		, - cast( w1.selling_general_and_administrative as float ) / 1000000 as "selling_general_and_administrative_expense"
		, - cast( w1.advertising_and_promotional as float ) / 1000000 as "advertising_and_promotional_expense"
		, null / 1000000 as "research_and_development"
		, null / 1000000 as "depreciation_amortisation"
		, null / 1000000 as "capex"
		, cast( w1.loss_from_operations as float ) / 1000000 as "operating_income"
		, ( - coalesce( cast( w1.interest_expense_bank_debt as float ), 0 )
		  - coalesce( cast( w1.interest_expense_shareholder as float ), 0 )
		  - coalesce( cast( w1.interest_expense_other as float ), 0 ) ) / 1000000
		  as "interest_expense"
		, cast( w1.provision_for_income_tax as float ) / 1000000 as "income_tax"
		, cast( w1.net_loss as float ) / 1000000 as "continuing_net_earnings_total"
		, null / 1000000 as "discontinued_net_earnings_total"
		, cast( w1.net_loss_attributable_to_common_stockholders as float ) / 1000000 as "net_earnings_to_shareholder"
	from
		"10_K2021_10_13_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_55406211321787" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( cast( w1.common_stock_outstanding as float ) / 1000000, 1 ) as "common_stock_outstanding"
		from
			"10_K2021_10_13_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICALS_55406211321787" w1
		where
			 w1.common_stock_outstanding is not null
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
		 		"10_K2021_10_13_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_55406211321787" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, cast( w1.revenues as float ) / 1000000 as "total_sales"
		, - cast( w1.cost_of_product_sold as float ) / 1000000 as "total_cost_of_sales"
		, - cast( w1.selling_general_and_administrative as float ) / 1000000 as "selling_general_and_administrative_expense"
		, - cast( w1.advertising_and_promotional as float ) / 1000000 as "advertising_and_promotional_expense"
		, null / 1000000 as "research_and_development"
		, null / 1000000 as "depreciation_amortisation"
		, null / 1000000 as "capex"
		, cast( w1.loss_from_operations as float ) / 1000000 as "operating_income"
		, ( - coalesce( cast( w1.interest_expense_bank_debt as float ), 0 )
		  - coalesce( cast( w1.interest_expense_shareholder as float ), 0 )
		  - coalesce( cast( w1.interest_expense_other as float ), 0 ) ) / 1000000
		  as "interest_expense"
		, cast( w1.provision_for_income_tax as float ) / 1000000 as "income_tax"
		, cast( w1.net_loss as float ) / 1000000 as "continuing_net_earnings_total"
		, null / 1000000 as "discontinued_net_earnings_total"
		, cast( w1.net_loss as float ) / 1000000 as "net_earnings_to_shareholder"
	from
		"10_K2019_10_15_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_55406191151122" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( cast( w1.common_stock_shares_outstanding as float ) / 1000000, 1 ) as "common_stock_outstanding"
		from
			"10_K2019_10_15_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_55406191151122" w1
		where
			 w1.common_stock_shares_outstanding is not null
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
		 		"10_K2019_10_15_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_55406191151122" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, cast( w1.revenues as float ) / 1000000 as "total_sales"
		, - cast( w1.cost_of_product_sold as float ) / 1000000 as "total_cost_of_sales"
		, - cast( w1.selling_general_and_administrative as float ) / 1000000 as "selling_general_and_administrative_expense"
		, - cast( w1.advertising_and_promotional as float ) / 1000000 as "advertising_and_promotional_expense"
		, null / 1000000 as "research_and_development"
		, null / 1000000 as "depreciation_amortisation"
		, null / 1000000 as "capex"
		, cast( w1.loss_from_operations as float ) / 1000000 as "operating_income"
		, ( - coalesce( cast( w1.interest_expense_bank_debt as float ), 0 )
		  - coalesce( cast( w1.interest_expense_shareholder as float ), 0 ) ) / 1000000
		  as "interest_expense"
		, cast( w1.provision_for_income_tax as float ) / 1000000 as "income_tax"
		, cast( w1.net_loss as float ) / 1000000 as "continuing_net_earnings_total"
		, null / 1000000 as "discontinued_net_earnings_total"
		, cast( w1.net_loss as float ) / 1000000 as "net_earnings_to_shareholder"
	from
		"10_K2017_10_03_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_55406171117422" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( cast( w1.common_stock_shares_outstanding as float ) / 1000000, 1 ) as "common_stock_outstanding"
		from
			"10_K2017_10_03_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_55406171117422" w1
		where
			 w1.common_stock_shares_outstanding is not null
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
		 		"10_K2017_10_03_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_55406171117422" a1
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
	, round( cast( ( w1.total_sales + w1.total_cost_of_sales ) as float ) / w1.total_sales * 100, 1 ) as "gpm_pct"
	, round( cast( w1.selling_general_and_administrative_expense as float ) / w1.total_sales * 100, 1 ) as "sga_pct"
	, round( cast( w1.operating_income as float ) / w1.total_sales * 100, 1 ) as "op_earnings_margin_pct"
	, round( cast( w1.total_sales as float ) / w1.common_stock_outstanding, 1 ) as "sales_per_share"
	, round( cast( w1.total_sales + w1.total_cost_of_sales as float ) / w1.common_stock_outstanding, 1 ) as "gp_per_share"
	, round( cast( w1.selling_general_and_administrative_expense as float ) / w1.common_stock_outstanding, 1 ) as "sga_per_share"
	, round( cast( w1.net_earnings_to_shareholder as float ) / w1.common_stock_outstanding, 1 ) as "earnings_per_share"
from
	temp_is w1
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
