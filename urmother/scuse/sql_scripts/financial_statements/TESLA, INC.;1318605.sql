
--================================
--	COMPANY NAME : TESLA, INC.		--UPDATE HERE
--	CENTRAL INDEX KEY : 1318605		--UPDATE HERE
--================================

drop table temp_cik;
create temporary table temp_cik as
select
	 '1318605' as "cik"		--UPDATE HERE
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
	, w1.*
from(
	--INSERT QUERY HERE
	select
		 w1.date
		, w1.cash_and_cash_equivalents as "cash_and_equivalents"
		, w1.accounts_receivable_net as "accounts_receivable_net"
		, w1.inventory as "inventories"
		, w1.total_current_assets as "total_current_assets"
		, coalesce( w1.property_plant_and_equipment_net, 0 )
		  + coalesce( w1.operating_lease_right_of_use_assets, 0 )
		  as "property_plant_and_equipment_net"
		, w1.goodwill as "goodwill"
		, w1.intangible_assets_net as "intangible_assets"
		, w1.total_assets as "total_assets"
		, w1.current_portion_of_debt_and_finance_leases as "short_term_debt"
		, coalesce( w1.deferred_revenue, 0 )
		  + coalesce( w1.customer_deposits, 0 )
		  as "short_term_unearned_revenue"
		, w1.accounts_payable as "accounts_payable"
		, null as "short_term_resale_value_guarantees"
		, w1.total_current_liabilities as "total_current_liabilities"
		, w1.deferred_revenue_net_of_current_portion as "long_term_unearned_revenue"
		, w1.debt_and_finance_leases_net_of_current_portion as "long_term_debt"
		, coalesce( w1.current_portion_of_debt_and_finance_leases, 0 )
		  + coalesce( w1.debt_and_finance_leases_net_of_current_portion, 0 ) as "total_debt"
		, null as "long_term_resale_value_guarantees"
		, w1.total_liabilities as "total_liabilities"
		, coalesce( w1.redeemable_noncontrolling_interests_in_subsidiaries, 0 )
		  + coalesce( w1.convertible_senior_notes_note_11, 0 )
		  as "convertible_notes"
		, coalesce( w1.common_stock_0_001_par_value_2000_shares_authorized_1033_and_960_shares_issued_and_outstanding_as_of_december_31_2021_and_december_31_2020_respectively, 0 )
		  + coalesce( w1.additional_paid_in_capital, 0 )
		  as "total_paid_in_capital"
		, null as "treasury_stock"
		, w1.retained_earnings_accumulated_deficit as "retained_earnings"
		, w1.total_stockholders_equity as "total_shareholder_equity"
		, w1.noncontrolling_interests_in_subsidiaries as "non_controlling_interest"
		, w1.total_liabilities_and_equity
	from
		"10_K2022_02_07_CONSOLIDATED_BALANCE_SHEETS_13475622595227" w1
	where
		 lower( w1.date ) not like '%consolidated%'
		union all
	select
		 w1.date
		, coalesce( w1.cash_and_cash_equivalents, 0 )
		  + coalesce( w1.restricted_cash, 0 )
		  + coalesce( w1.restricted_cash_net_of_current_portion, 0 )
		  as "cash_and_equivalents"
		, w1.accounts_receivable_net as "accounts_receivable_net"
		, w1.inventory as "inventories"
		, w1.total_current_assets as "total_current_assets"
		, coalesce( w1.property_plant_and_equipment_net, 0 )
		  + coalesce( w1.operating_lease_right_of_use_assets, 0 )
		  as "property_plant_and_equipment_net"
		, w1.goodwill as "goodwill"
		, w1.intangible_assets_net as "intangible_assets"
		, w1.total_assets as "total_assets"
		, w1.current_portion_of_debt_and_finance_leases as "short_term_debt"
		, coalesce( w1.deferred_revenue, 0 )
		  + coalesce( w1.customer_deposits, 0 )
		  as "short_term_unearned_revenue"
		, w1.accounts_payable as "accounts_payable"
		, null as "short_term_resale_value_guarantees"
		, w1.total_current_liabilities as "total_current_liabilities"
		, w1.deferred_revenue_net_of_current_portion as "long_term_unearned_revenue"
		, w1.debt_and_finance_leases_net_of_current_portion as "long_term_debt"
		, coalesce( w1.current_portion_of_debt_and_finance_leases, 0 )
		  + coalesce( w1.debt_and_finance_leases_net_of_current_portion, 0 )
		  as "total_debt"
		, null as "long_term_resale_value_guarantees"
		, w1.total_liabilities as "total_liabilities"
		, w1.redeemable_noncontrolling_interests_in_subsidiaries as "convertible_notes"
		, coalesce( w1.common_stock_0_001_par_value_2000_shares_authorized_181_and_173_shares_issued_and_outstanding_as_of_december_31_2019_and_2018_respectively, 0 )
		  + coalesce( w1.additional_paid_in_capital, 0 )
		  as "total_paid_in_capital"
		, null as "treasury_stock"
		, w1.accumulated_deficit as "retained_earnings"
		, w1.total_stockholders_equity as "total_shareholder_equity"
		, w1.noncontrolling_interests_in_subsidiaries as "non_controlling_interest"
		, w1.total_liabilities_and_equity
	from
		"10_K2020_02_13_CONSOLIDATED_BALANCE_SHEETS_13475620606921" w1
		union all
	select
		 w1.date
		, ( coalesce( w1.cash_and_cash_equivalents, 0 )
		  + coalesce( w1.restricted_cash, 0 )
		  + coalesce( w1.restricted_cash_net_of_current_portion, 0 ) ) / 1000
		  as "cash_and_equivalents"
		, w1.accounts_receivable_net / 1000 as "accounts_receivable_net"
		, w1.inventory / 1000 as "inventories"
		, w1.total_current_assets / 1000 as "total_current_assets"
		, w1.property_plant_and_equipment_net / 1000 as "property_plant_and_equipment_net"
		, w1.goodwill / 1000 as "goodwill"
		, w1.intangible_assets_net / 1000 as "intangible_assets"
		, w1.total_assets / 1000 as "total_assets"
		, w1.current_portion_of_long_term_debt_and_capital_leases / 1000 as "short_term_debt"
		, ( coalesce( w1.deferred_revenue, 0 )
		  + coalesce( w1.customer_deposits, 0 ) ) / 1000
		  as "short_term_unearned_revenue"
		, w1.accounts_payable / 1000 as "accounts_payable"
		, w1.resale_value_guarantees / 1000 as "short_term_resale_value_guarantees"
		, w1.total_current_liabilities / 1000 as "total_current_liabilities"
		, w1.deferred_revenue_net_of_current_portion / 1000 as "long_term_unearned_revenue"
		, ( coalesce( w1.long_term_debt_and_capital_leases_net_of_current_portion, 0 )
		  + coalesce( w1.convertible_senior_notes_issued_to_related_parties, 0 ) ) / 1000
		  as "long_term_debt"
		, ( coalesce( w1.current_portion_of_long_term_debt_and_capital_leases, 0 )
		  + coalesce( w1.long_term_debt_and_capital_leases_net_of_current_portion, 0 )
		  + coalesce( w1.convertible_senior_notes_issued_to_related_parties, 0 ) ) / 1000
		  as "total_debt"
		, w1.resale_value_guarantees_net_of_current_portion / 1000 as "long_term_resale_value_guarantees"
		, w1.total_liabilities / 1000 as "total_liabilities"
		, ( coalesce( w1.convertible_senior_notes_note_13,0 )
		  + coalesce( w1.redeemable_noncontrolling_interests_in_subsidiaries, 0 ) ) / 1000
		  as "convertible_notes"
		, ( coalesce( w1.common_stock_0_001_par_value_2000000_shares_authorized_168797_and_161561_shares_issued_and_outstanding_as_of_december_31_2017_and_december_31_2016_respectively, 0 )
		  + coalesce( w1.additional_paid_in_capital, 0 ) ) / 1000
		  as "total_paid_in_capital"
		, null / 1000 as "treasury_stock"
		, w1.accumulated_deficit / 1000 as "retained_earnings"
		, w1.total_stockholders_equity / 1000 as "total_shareholder_equity"
		, w1.noncontrolling_interests_in_subsidiaries / 1000 as "non_controlling_interest"
		, w1.total_liabilities_and_equity / 1000
	from
		"10_K2018_02_23_CONSOLIDATED_BALANCE_SHEETS_13475618634585" w1
		union all
	select
		 w1.date
		, ( coalesce( w1.cash_and_cash_equivalents, 0 )
		  + coalesce( w1.restricted_cash_and_marketable_securities, 0 )
		  + coalesce( w1.restricted_cash, 0 ) ) / 1000
		  as "cash_and_equivalents"
		, w1.accounts_receivable / 1000 as "accounts_receivable_net"
		, w1.inventory / 1000 as "inventories"
		, w1.total_current_assets / 1000 as "total_current_assets"
		, w1.property_plant_and_equipment_net / 1000 as "property_plant_and_equipment_net"
		, null / 1000 as "goodwill"
		, null / 1000 as "intangible_assets"
		, w1.total_assets / 1000 as "total_assets"
		, w1.long_term_debt_and_capital_leases / 1000 as "short_term_debt"
		, ( coalesce( w1.deferred_revenue, 0 )
		  + coalesce( w1.customer_deposits, 0 ) ) / 1000
		  as "short_term_unearned_revenue"
		, w1.accounts_payable / 1000 as "accounts_payable"
		, w1.resale_value_guarantees / 1000 as "short_term_resale_value_guarantees"
		, w1.total_current_liabilities / 1000 as "total_current_liabilities"
		, w1."deferred_revenue.1" / 1000 as "long_term_unearned_revenue"
		, w1."long_term_debt_and_capital_leases.1" / 1000 as "long_term_debt"
		, ( coalesce( w1.long_term_debt_and_capital_leases, 0 )
		  + coalesce( w1."long_term_debt_and_capital_leases.1", 0 ) ) / 1000
		  as "total_debt"
		, w1.resale_value_guarantee /  1000 as "long_term_resale_value_guarantees"
		, w1.total_liabilities / 1000 as "total_liabilities"
		, w1.convertible_senior_notes_notes_8 / 1000 as "convertible_notes"
		, ( coalesce( w1.common_stock_0_001_par_value_2000000_shares_authorized_as_of_december_31_2015_and_2014_respectively_131425_and_125688_shares_issued_and_outstanding_as_of_december_31_2015_and_2014_respectively, 0 )
		  + coalesce( w1.additional_paid_in_capital, 0 ) ) / 1000
		  as "total_paid_in_capital"
		, null / 1000 as "treasury_stock"
		, w1.accumulated_deficit / 1000 as "retained_earnings"
		, w1.total_stockholders_equity / 1000 as "total_shareholder_equity"
		, null / 1000 as "non_controlling_interest"
		, w1.total_liabilities_and_stockholders_equity / 1000
	from
		"10_K2016_02_24_CONSOLIDATED_BALANCE_SHEETS_134756161452193" w1
		union all
	select
		 w1.date
		, ( coalesce( w1.cash_and_cash_equivalents, 0 )
		  + coalesce( w1.restricted_cash, 0 )
		  + coalesce( w1."restricted_cash.1", 0 ) ) / 1000
		  as "cash_and_equivalents"
		, w1.accounts_receivable / 1000 as "accounts_receivable_net"
		, w1.inventory / 1000 as "inventories"
		, w1.total_current_assets / 1000 as "total_current_assets"
		, w1.property_plant_and_equipment_net / 1000 as "property_plant_and_equipment_net"
		, null / 1000 as "goodwill"
		, null / 1000 as "intangible_assets"
		, w1.total_assets / 1000 as "total_assets"
		, ( coalesce( w1.capital_lease_obligations_current_portion, 0 )
		  + coalesce( w1.convertible_debt_current_portion, 0 )
		  + coalesce( w1.long_term_debt_current_portion, 0 ) ) / 1000
		  as "short_term_debt"
		, ( coalesce( w1.deferred_revenue, 0 )
		  + coalesce( w1.customer_deposits, 0 ) ) / 1000
		  as "short_term_unearned_revenue"
		, w1.accounts_payable / 1000 as "accounts_payable"
		, null / 1000 as "short_term_resale_value_guarantees"
		, w1.total_current_liabilities / 1000 as "total_current_liabilities"
		, w1.deferred_revenue_less_current_portion / 1000 as "long_term_unearned_revenue"
		, ( coalesce( w1.capital_lease_obligations_less_current_portion, 0 )
		  + coalesce( w1.convertible_debt_less_current_portion, 0 )
		  + coalesce( w1.long_term_debt_less_current_portion, 0 ) ) / 1000
		  as "long_term_debt"
		, ( coalesce( w1.capital_lease_obligations_current_portion, 0 )
		  + coalesce( w1.convertible_debt_current_portion, 0 )
		  + coalesce( w1.long_term_debt_current_portion, 0 )
		  + coalesce( w1.capital_lease_obligations_less_current_portion, 0 )
		  + coalesce( w1.convertible_debt_less_current_portion, 0 )
		  + coalesce( w1.long_term_debt_less_current_portion, 0 ) ) / 1000
		  as "total_debt"
		, w1.resale_value_guarantee / 1000 as "long_term_resale_value_guarantees"
		, w1.total_liabilities / 1000 as "total_liabilities"
		, null / 1000 as "convertible_notes"
		, ( coalesce( w1.common_stock_0_001_par_value_2000000000_shares_authorized_as_of_december_31_2013_and_2012_respectively_123090990_and_114214274_shares_issued_and_outstanding_as_of_december_31_2013_and_2012_respectively, 0 )
		  + coalesce( w1.additional_paid_in_capital, 0 ) ) / 1000
		  as "total_paid_in_capital"
		, null / 1000 as "treasury_stock"
		, w1.accumulated_deficit / 1000 as "retained_earnings"
		, w1.total_stockholders_equity / 1000 as "total_shareholder_equity"
		, null / 1000 as "non_controlling_interest"
		, w1.total_liabilities_and_stockholders_equity / 1000
	from
		"10_K2014_02_26_CONSOLIDATED_BALANCE_SHEETS_13475614644610" w1
) w1
order by
	 date
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
		, w1.total_revenues as "total_sales"
		, - w1.total_cost_of_revenues as "total_cost_of_sales"
		, - w1.selling_general_and_administrative as "selling_general_and_administrative_expense"
		, - w1.research_and_development as "research_and_development"
		, null as "depreciation_amortisation"
		, null as "capex"
		, w1.income_loss_from_operations as "operating_income"
		, w1.interest_expense
		, - w1.provision_for_income_taxes as "income_tax"
		, w1.net_income_loss as "continuing_net_earnings_total"
		, null as "discontinued_net_earnings_total"
		, w1.net_income_loss_attributable_to_common_stockholders as "net_earnings_to_shareholder"
	from
		"10_K2022_02_07_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13475622595227" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 date( w1.date, 'start of month', '+1 month', '-1 day' ) as "end_of_month"
			, round(
			  	 w1.common_stock_shares_outstanding
			  	 / 1000000
			  	, 1
			  ) as "common_stock_outstanding"
		from
			"10_K2022_02_07_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_13475622595227" w1
		where
			 w1.common_stock_shares_outstanding is not null
		order by
			 date( w1.date, 'start of month', '+1 month', '-1 day' ) desc
		limit 3
	)																			w2 on
			 w1.date = w2.end_of_month
	where
		 w1.date in(
		 	--TO FILTER RECENT 2 YEARS
		 	select
		 		 a1.date
		 	from
		 		"10_K2022_02_07_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13475622595227" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, w1.total_revenues as "total_sales"
		, - w1.total_cost_of_revenues as "total_cost_of_sales"
		, - w1.selling_general_and_administrative as "selling_general_and_administrative_expense"
		, - w1.research_and_development as "research_and_development"
		, null as "depreciation_amortisation"
		, null as "capex"
		, w1.loss_from_operations as "operating_income"
		, w1.interest_expense
		, - w1.provision_for_income_taxes as "income_tax"
		, w1.net_loss as "continuing_net_earnings_total"
		, null as "discontinued_net_earnings_total"
		, w1.net_loss_attributable_to_common_stockholders as "net_earnings_to_shareholder"
	from
		"10_K2020_02_13_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13475620606921" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 date( w1.date, 'start of month', '+1 month', '-1 day' ) as "end_of_month"
			, round(
			  	 w1.common_stock_shares_outstanding
			  	 * 5	--5 for 1 stock split in 2020-08
			  	 / 1000000
			  	, 1
			  ) as "common_stock_outstanding"
		from
			"10_K2020_02_13_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_13475620606921" w1
		where
			 w1.common_stock_shares_outstanding is not null
		order by
			 date( w1.date, 'start of month', '+1 month', '-1 day' ) desc
		limit 3
	)																			w2 on
			 w1.date = w2.end_of_month
	where
		 w1.date in(
		 	--TO FILTER RECENT 2 YEARS
		 	select
		 		 a1.date
		 	from
		 		"10_K2020_02_13_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13475620606921" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, w1.total_revenues / 1000 as "total_sales"
		, - w1.total_cost_of_revenues / 1000 as "total_cost_of_sales"
		, - w1.selling_general_and_administrative / 1000 as "selling_general_and_administrative_expense"
		, - w1.research_and_development / 1000 as "research_and_development"
		, null / 1000 as "depreciation_amortisation"
		, null / 1000 as "capex"
		, w1.loss_from_operations / 1000 as "operating_income"
		, w1.interest_expense / 1000
		, - w1.provision_for_income_taxes / 1000 as "income_tax"
		, w1.net_loss / 1000 as "continuing_net_earnings_total"
		, null / 1000 as "discontinued_net_earnings_total"
		, w1.net_loss_attributable_to_common_stockholders / 1000 as "net_earnings_to_shareholder"
	from
		"10_K2018_02_23_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13475618634585" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 date( w1.date, 'start of month', '+1 month', '-1 day' ) as "end_of_month"
			, round(
			  	 w1.common_stock_shares_outstanding
			  	 * 5	--5 for 1 stock split in 2020-08
			  	 / 1000000
			  	, 1
			  ) as "common_stock_outstanding"
		from
			"10_K2018_02_23_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_13475618634585" w1
		where
			 w1.common_stock_shares_outstanding is not null
		order by
			 date( w1.date, 'start of month', '+1 month', '-1 day' ) desc
		limit 3
	)																			w2 on
			 w1.date = w2.end_of_month
	where
		 w1.date in(
		 	--TO FILTER RECENT 2 YEARS
		 	select
		 		 a1.date
		 	from
		 		"10_K2018_02_23_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13475618634585" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, w1.total_revenues / 1000 as "total_sales"
		, - w1.total_cost_of_revenues / 1000 as "total_cost_of_sales"
		, - w1.selling_general_and_administrative / 1000 as "selling_general_and_administrative_expense"
		, - w1.research_and_development / 1000 as "research_and_development"
		, null / 1000 as "depreciation_amortisation"
		, null / 1000 as "capex"
		, w1.loss_from_operations / 1000 as "operating_income"
		, w1.interest_expense / 1000
		, - w1.provision_for_income_taxes / 1000 as "income_tax"
		, w1.net_loss / 1000 as "continuing_net_earnings_total"
		, null / 1000 as "discontinued_net_earnings_total"
		, w1.net_loss / 1000 as "net_earnings_to_shareholder"
	from
		"10_K2016_02_24_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_134756161452193" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 date( w1.date, 'start of month', '+1 month', '-1 day' ) as "end_of_month"
			, round(
			  	 w1.common_stock_shares_outstanding
			  	 * 5	--5 for 1 stock split in 2020-08
			  	 / 1000000
			  	, 1
			  ) as "common_stock_outstanding"
		from
			"10_K2016_02_24_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_134756161452193" w1
		where
			 w1.common_stock_shares_outstanding is not null
		order by
			 date( w1.date, 'start of month', '+1 month', '-1 day' ) desc
		limit 3
	)																			w2 on
			 w1.date = w2.end_of_month
	where
		 w1.date in(
		 	--TO FILTER RECENT 2 YEARS
		 	select
		 		 a1.date
		 	from
		 		"10_K2016_02_24_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_134756161452193" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
		union all
	select
		 w1.date
		, w2.common_stock_outstanding
		, w1.total_revenues / 1000 as "total_sales"
		, - w1.total_cost_of_revenues / 1000 as "total_cost_of_sales"
		, - w1.selling_general_and_administrative / 1000 as "selling_general_and_administrative_expense"
		, - w1.research_and_development / 1000 as "research_and_development"
		, null / 1000 as "depreciation_amortisation"
		, null / 1000 as "capex"
		, w1.loss_from_operations / 1000 as "operating_income"
		, w1.interest_expense / 1000
		, - w1.provision_for_income_taxes / 1000 as "income_tax"
		, w1.net_loss / 1000 as "continuing_net_earnings_total"
		, null / 1000 as "discontinued_net_earnings_total"
		, w1.net_loss / 1000 as "net_earnings_to_shareholder"
	from
		"10_K2014_02_26_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13475614644610" 	w1
	left join(
		--TO GET LATEST 3 YEARS OF COMMON STOCK OUTSTANDING
		select
			 date( w1.date, 'start of month', '+1 month', '-1 day' ) as "end_of_month"
			, round(
			  	 w1.common_stock_shares_outstanding
			  	 * 5	--5 for 1 stock split in 2020-08
			  	 / 1000000
			  	, 1
			  ) as "common_stock_outstanding"
		from
			"10_K2014_02_26_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_13475614644610" w1
		where
			 w1.common_stock_shares_outstanding is not null
		order by
			 date( w1.date, 'start of month', '+1 month', '-1 day' ) desc
		limit 3
	)																			w2 on
			 w1.date = w2.end_of_month
	where
		 w1.date in(
		 	--TO FILTER RECENT 2 YEARS
		 	select
		 		 a1.date
		 	from
		 		"10_K2014_02_26_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13475614644610" a1
		 	order by
		 		 a1.date desc
		 	limit 2
		 )
) e1
order by
	 date
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
