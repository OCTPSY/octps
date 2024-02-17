--================================
--	COMPANY NAME : KARAT PACKAGING INC.		--UPDATE HERE
--	CENTRAL INDEX KEY : 1758021		--UPDATE HERE
--================================

drop table temp_cik;
create temporary table temp_cik as
select
	 '1758021' as "cik"		--UPDATE HERE
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
	 group_concat( 'select * from "' || w1.name || '"', ' union all ' ) as "bs_query"
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
	 group_concat( 'select * from "' || w1.name || '"', ' union all ' ) as "is_query"
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
		, w1.cash_and_cash_equivalents_including_1_2_million_and_0_1_million_associated_with_variable_interest_entity_at_december_31_2021_and_2020_respectively / 1000000 as "cash_and_equivalents"
		, w1.accounts_receivable_net_of_allowance_for_doubtful_accounts_of_0_3_million_at_both_december_31_2021_and_2020 / 1000000 as "accounts_receivable_net"
		, w1.inventories / 1000000 as "inventories"
		, w1.total_current_assets / 1000000 as "total_current_assets"
		, w1.property_and_equipment_net_including_46_6_million_and_47_8_million_associated_with_variable_interest_entity_at_december_31_2021_and_2020_respectively / 1000000 as "property_plant_and_equipment_net"
		, w1.goodwill / 1000000 as "goodwill"
		, w1.intangible_assets_net / 1000000 as "intangible_assets"
		, w1.total_assets / 1000000 as "total_assets"
		, ( coalesce( w1.debt_current_portion_including_1_2_million_and_0_7_million_associated_with_variable_interest_entity_at_december_31_2021_and_2020_respectively, 0 )
		  + coalesce( w1.capital_leases_current_portion, 0 )
		  + coalesce( w1.credit_cards_payable, 0 ) ) / 1000000
		  as "short_term_debt"
		, w1.customer_deposits_including_0_1_million_and_0_0_million_associated_with_variable_interest_entity_at_december_31_2021_and_2020_respectively / 1000000 as "short_term_unearned_revenue"
		, w1.accounts_payable_including_0_1_million_and_0_6_million_associated_with_variable_interest_entity_at_december_31_2021_and_2020_respectively / 1000000 as "accounts_payable"
		, w1.total_current_liabilities / 1000000 as "total_current_liabilities"
		, null / 1000000 as "long_term_unearned_revenue"
		, ( coalesce( w1.long_term_debt_net_of_current_portion_and_debt_discount_of_0_2_million_and_0_1_million_at_december_31_2021_and_december_31_2020_respectively_including_35_3_million_and_36_7_million_associated_with_variable_interest_entity_at_december_31_2021_and_2020_respectively_and_debt_discount_of_0_2_million_and_0_1_million_associated_with_variable_interest_entity_at_december_31_2021_and_2020_respectively, 0 )
		  + coalesce( w1.capital_leases_net_of_current_portion, 0 )
		  + coalesce( w1.line_of_credit, 0 ) ) / 1000000
		  as "long_term_debt"
		, ( coalesce( w1.debt_current_portion_including_1_2_million_and_0_7_million_associated_with_variable_interest_entity_at_december_31_2021_and_2020_respectively, 0 )
		  + coalesce( w1.capital_leases_current_portion, 0 )
		  + coalesce( w1.credit_cards_payable, 0 )
		  + coalesce( w1.long_term_debt_net_of_current_portion_and_debt_discount_of_0_2_million_and_0_1_million_at_december_31_2021_and_december_31_2020_respectively_including_35_3_million_and_36_7_million_associated_with_variable_interest_entity_at_december_31_2021_and_2020_respectively_and_debt_discount_of_0_2_million_and_0_1_million_associated_with_variable_interest_entity_at_december_31_2021_and_2020_respectively, 0 )
		  + coalesce( w1.capital_leases_net_of_current_portion, 0 )
		  + coalesce( w1.line_of_credit, 0 ) ) / 1000000
		  as "total_debt"
		, w1.total_liabilities / 1000000 as "total_liabilities"
		, ( coalesce( w1.common_stock_0_001_par_value_100000000_shares_authorized_19827417_and_19804417_shares_issued_and_outstanding_respectively_as_of_december_31_2021_and_15190000_and_15167000_shares_issued_and_outstanding_respectively_as_of_december_31_2020, 0 )
		  + coalesce( w1.additional_paid_in_capital, 0 ) ) / 1000000
		  as "total_paid_in_capital"
		, w1.treasury_stock_0_001_par_value_23000_shares_as_of_both_december_31_2021_and_2020 / 1000000 as "treasury_stock"
		, w1.retained_earnings / 1000000 as "retained_earnings"
		, w1.total_karat_packaging_inc_stockholders_equity / 1000000 as "total_shareholder_equity"
		, w1.noncontrolling_interest / 1000000 as "non_controlling_interest"
		, w1.total_liabilities_and_stockholders_equity / 1000000 as "total_liabilities_and_equity"
	from
		"10_K2022_03_31_CONSOLIDATED_BALANCE_SHEETS_14033622791404" w1
--		union all
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
		, w1.net_sales / 1000000 as "total_sales"
		, - w1.cost_of_goods_sold / 1000000 as "total_cost_of_sales"
		, ( - coalesce( w1.selling_expenses, 0 )
		  - coalesce( w1.general_and_administrative_expenses_including_2_5_million_and_2_0_million_associated_with_variable_interest_entity_for_the_years_ended_december_31_2021_and_2020_respectively, 0 ) ) / 1000000
		  as "selling_general_and_administrative_expense"
		, null / 1000000 as "research_and_development"
		, null / 1000000 as "depreciation_amortisation"
		, null / 1000000 as "capex"
		, w1.operating_income / 1000000 as "operating_income"
		, - w1.interest_expense_net_including_0_5_million_and_2_9_million_associated_with_variable_interest_entity_for_the_years_ended_december_31_2021_and_2020_respectively / 1000000 as "interest_expense"
		, - w1.provision_for_income_taxes / 1000000 as "income_tax"
		, w1.net_income / 1000000 as "continuing_net_earnings_total"
		, null / 1000000 as "discontinued_net_earnings_total"
		, w1.net_income_attributable_to_karat_packaging_inc / 1000000 as "net_earnings_to_shareholder"
	from
		"10_K2022_03_31_CONSOLIDATED_STATEMENTS_OF_INCOME_14033622791404" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 w1.date as "end_of_month"
			, round( cast( w1.common_stock_shares_outstanding_in_shares as float ) / 1000000, 1 ) as "common_stock_outstanding"
		from
			"10_K2022_03_31_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_14033622791404" w1
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
		 		"10_K2022_03_31_CONSOLIDATED_STATEMENTS_OF_INCOME_14033622791404" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
--		union all
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
