
--=====================================================
--	COMPANY NAME : UNITED AIRLINES HOLDINGS, INC.
--	CENTRAL INDEX KEY : 100517
--=====================================================

delete from "10_K2020_02_25_STATEMENTS_OF_CONSOLIDATED_OPERATIONS_10603320647075"
where
	 revenue = '[1]'
;

delete from "10_K2020_02_25_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10603320647075"
where
	 receivables_allowance_for_doubtful_accounts = '[1]'
;


with
	temp_balance_sheet as(
		--UPDATE HERE
select 'UAL;100517' as "ticker_cik", '10_K2022_02_18_CONSOLIDATED_BALANCE_SHEETS_10603322652957' as "table_name", date as "date",  ( (  + cast( coalesce( "cash_and_cash_equivalents", 0 ) as float ) + cast( coalesce( "restricted_cash", 0 ) as float ) + cast( coalesce( "restricted_cash.1", 0 ) as float ) ) / 1 ) as "cash_and_equivalents", ( (  + cast( coalesce( "receivables_less_allowance_for_credit_losses_2021_28_2020_78", 0 ) as float ) ) / 1 ) as "accounts_receivable_net", ( (  + cast( coalesce( "aircraft_fuel_spare_parts_and_supplies_less_obsolescence_allowance_2021_546_2020_478", 0 ) as float ) ) / 1 ) as "inventories", ( (  + cast( coalesce( "short_term_investments", 0 ) as float ) ) / 1 ) as "investments", ( (  + cast( coalesce( "total_current_assets", 0 ) as float ) ) / 1 ) as "total_current_assets", ( (  + cast( coalesce( "total_operating_property_and_equipment_net", 0 ) as float ) + cast( coalesce( "operating_lease_right_of_use_assets", 0 ) as float ) ) / 1 ) as "property_plant_and_equipment_net", ( (  + cast( coalesce( "goodwill", 0 ) as float ) ) / 1 ) as "goodwill", ( (  + cast( coalesce( "intangibles_less_accumulated_amortization_2021_1544_2020_1495", 0 ) as float ) ) / 1 ) as "intangible_assets", ( (  + cast( coalesce( "total_assets", 0 ) as float ) ) / 1 ) as "total_assets", ( (  + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) + cast( coalesce( "current_maturities_of_other_financial_liabilities", 0 ) as float ) + cast( coalesce( "current_maturities_of_operating_leases", 0 ) as float ) + cast( coalesce( "current_maturities_of_finance_leases", 0 ) as float ) ) / 1 ) as "short_term_debt", ( (  + cast( coalesce( "advance_ticket_sales", 0 ) as float ) + cast( coalesce( "frequent_flyer_deferred_revenue", 0 ) as float ) ) / 1 ) as "short_term_unearned_revenue", ( (  + cast( coalesce( "accounts_payable", 0 ) as float ) ) / 1 ) as "accounts_payable", ( (  + cast( coalesce( "total_current_liabilities", 0 ) as float ) ) / 1 ) as "total_current_liabilities", ( (  + cast( coalesce( "frequent_flyer_deferred_revenue.1", 0 ) as float ) ) / 1 ) as "long_term_unearned_revenue", ( (  
+ cast( coalesce( "long_term_debt", 0 ) as float ) + cast( coalesce( "long_term_obligations_under_operating_leases", 0 ) as float ) + cast( coalesce( "long_term_obligations_under_finance_leases", 0 ) as float ) ) / 1 ) as "long_term_debt", ( (  + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) + cast( coalesce( "current_maturities_of_other_financial_liabilities", 0 ) as float ) + cast( coalesce( "current_maturities_of_operating_leases", 0 ) as float ) + cast( coalesce( "current_maturities_of_finance_leases", 0 ) as float ) + cast( coalesce( "long_term_debt", 0 ) as float ) + cast( coalesce( "long_term_obligations_under_operating_leases", 0 ) as float ) + cast( coalesce( "long_term_obligations_under_finance_leases", 0 ) as float ) ) / 1 ) as "total_debt", ( null ) as "total_liabilities", ( (  + cast( coalesce( "common_stock", 0 ) as float ) + cast( coalesce( "additional_capital_invested", 0 ) as float ) ) / 1 ) as "total_paid_in_capital", ( (  + cast( coalesce( "stock_held_in_treasury_at_cost", 0 ) as float ) ) / 1 ) as "treasury_stock", ( (  + cast( coalesce( "retained_earnings", 0 ) as float ) ) / 1 ) as "retained_earnings", ( (  + cast( coalesce( "total_stockholders_equity", 0 ) as float ) ) / 1 ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( (  + cast( coalesce( "total_liabilities_and_stockholders_equity", 0 ) as float ) ) / 1 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2022_02_18_CONSOLIDATED_BALANCE_SHEETS_10603322652957" union all select 'UAL;100517' as "ticker_cik", '10_K2020_02_25_CONSOLIDATED_BALANCE_SHEETS_10603320647075' as "table_name", date as "date",  ( (  + cast( coalesce( "cash_and_cash_equivalents", 0 ) as float ) + cast( coalesce( "restricted_cash", 0 ) as float ) ) / 1 ) as "cash_and_equivalents", ( (  + cast( coalesce( "receivables_less_allowance_for_doubtful_accounts_2019_9_2018_8", 0 ) as float ) ) / 1 ) as "accounts_receivable_net", ( (  + cast( coalesce( "aircraft_fuel_spare_parts_and_supplies_less_obsolescence_allowance_2019_425_2018_412", 0 ) as float ) ) / 1 ) as "inventories", ( (  + cast( coalesce( "short_term_investments", 0 ) as float ) ) / 1 ) as "investments", ( (  + cast( coalesce( "total_current_assets", 0 ) as float ) ) / 1 ) as "total_current_assets", ( (  + cast( coalesce( "total_operating_property_and_equipment_net", 0 ) as float ) + cast( coalesce( "operating_lease_right_of_use_assets", 0 ) as float ) ) / 1 ) as "property_plant_and_equipment_net", ( (  + cast( coalesce( "goodwill", 0 ) as float ) ) / 1 ) as "goodwill", ( (  + cast( coalesce( "intangibles_less_accumulated_amortization_2019_1440_2018_1380", 0 ) as float ) ) / 1 ) as "intangible_assets", ( (  + cast( coalesce( "total_assets", 0 ) as float ) ) / 1 ) as "total_assets", ( (  + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) + 
cast( coalesce( "current_maturities_of_finance_leases", 0 ) as float ) + cast( coalesce( "current_maturities_of_operating_leases", 0 ) as float ) ) / 1 ) as "short_term_debt", ( (  + cast( coalesce( "advance_ticket_sales", 0 ) as float ) + cast( coalesce( "frequent_flyer_deferred_revenue", 0 ) as float ) ) / 1 ) as "short_term_unearned_revenue", ( (  + cast( coalesce( "accounts_payable", 0 ) as float ) ) / 1 ) as "accounts_payable", ( (  + cast( coalesce( "total_current_liabilities", 0 ) as float ) 
) / 1 ) as "total_current_liabilities", ( (  + cast( coalesce( "frequent_flyer_deferred_revenue.1", 0 ) as float ) ) / 1 ) as 
"long_term_unearned_revenue", ( (  + cast( coalesce( "long_term_debt", 0 ) as float ) + cast( coalesce( "long_term_obligations_under_finance_leases", 0 ) as float ) + cast( coalesce( "long_term_obligations_under_operating_leases", 0 ) as float ) ) / 1 
) as "long_term_debt", ( (  + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as float ) + cast( coalesce( "current_maturities_of_finance_leases", 0 ) as float ) + cast( coalesce( "current_maturities_of_operating_leases", 0 ) as float ) + cast( coalesce( "long_term_debt", 0 ) as float ) + cast( coalesce( "long_term_obligations_under_finance_leases", 0 ) as float ) + cast( coalesce( "long_term_obligations_under_operating_leases", 0 ) as float ) ) / 1 ) as "total_debt", ( null ) as "total_liabilities", ( (  + cast( coalesce( "common_stock", 0 ) as float ) + cast( coalesce( "additional_capital_invested", 0 ) as float ) ) / 1 ) as "total_paid_in_capital", ( (  + cast( coalesce( "stock_held_in_treasury_at_cost", 0 ) as float ) ) / 1 ) as "treasury_stock", ( (  + cast( coalesce( "retained_earnings", 0 ) as float ) ) / 1 ) as "retained_earnings", ( (  + cast( coalesce( "total_stockholders_equity", 0 ) as float ) ) / 1 ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( (  + cast( coalesce( "total_liabilities_and_stockholders_equity", 0 ) as float ) ) / 1 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2020_02_25_CONSOLIDATED_BALANCE_SHEETS_10603320647075" union all select 'UAL;100517' as "ticker_cik", '10_K2018_02_22_CONSOLIDATED_BALANCE_SHEETS_10603318633334' as "table_name", date as "date",  ( (  + cast( coalesce( "cash_and_cash_equivalents", 0 ) as float ) + cast( coalesce( "restricted_cash", 0 ) as float ) ) / 1 ) as "cash_and_equivalents", ( (  + cast( coalesce( "receivables_less_allowance_for_doubtful_accounts_2017_7_2016_10", 0 ) as float ) ) / 1 ) as "accounts_receivable_net", ( (  + cast( coalesce( "aircraft_fuel_spare_parts_and_supplies_less_obsolescence_allowance_2017_354_2016_295", 0 ) as float ) ) / 1 ) as "inventories", ( (  + cast( coalesce( "short_term_investments", 0 ) 
as float ) ) / 1 ) as "investments", ( (  + cast( coalesce( "total_current_assets", 0 ) as float ) ) / 1 ) as "total_current_assets", ( (  + cast( coalesce( "total_owned_property_and_equipment_net", 0 ) as float ) + cast( coalesce( "purchase_deposits_for_flight_equipment", 0 ) as float ) + cast( coalesce( "total_capital_leases_net", 0 ) as float ) ) / 1 ) as "property_plant_and_equipment_net", ( (  + cast( coalesce( "goodwill", 0 ) as float ) ) / 1 ) as "goodwill", ( (  + cast( coalesce( "intangibles_less_accumulated_amortization_2017_1313_2016_1234", 0 ) as float ) ) / 1 ) as "intangible_assets", ( (  + cast( coalesce( "total_assets", 0 ) as float ) ) / 1 ) as "total_assets", ( (  + cast( coalesce( "current_maturities_of_long_term_debt", 0 ) as 
float ) + cast( coalesce( "current_maturities_of_capital_leases", 0 ) as float ) ) / 1 ) as "short_term_debt", ( (  + cast( coalesce( "advance_ticket_sales", 0 ) as float ) + cast( coalesce( "frequent_flyer_deferred_revenue", 0 ) as float ) ) / 1 ) as 
"short_term_unearned_revenue", ( (  + cast( coalesce( "accounts_payable", 0 ) as float ) ) / 1 ) as "accounts_payable", ( (  + cast( coalesce( "total_current_liabilities", 0 ) as float ) ) / 1 ) as "total_current_liabilities", ( (  + cast( coalesce( "frequent_flyer_deferred_revenue.1", 0 ) as float ) + cast( coalesce( "advanced_purchase_of_miles", 0 ) as float ) ) / 1 ) as "long_term_unearned_revenue", ( (  + cast( coalesce( "long_term_debt", 0 ) as float ) + cast( coalesce( "long_term_obligations_under_capital_leases", 0 ) as float ) + cast( coalesce( "lease_fair_value_adjustment_net", 0 ) as float ) ) / 1 ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( (  + cast( coalesce( "common_stock", 0 ) as float ) + cast( coalesce( "additional_capital_invested", 0 ) as float ) ) / 1 ) as "total_paid_in_capital", ( (  + cast( coalesce( "stock_held_in_treasury_at_cost", 0 ) as float ) ) / 1 ) as "treasury_stock", ( (  + cast( coalesce( "retained_earnings", 0 ) as float ) ) / 1 ) as "retained_earnings", ( (  + cast( coalesce( "total_stockholder_s_equity", 0 ) as float ) ) / 1 ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( (  + cast( coalesce( "total_liabilities_and_stockholder_s_equity", 0 ) as float ) ) / 1 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2018_02_22_CONSOLIDATED_BALANCE_SHEETS_10603318633334"
	)
	,
	temp_balance_sheet_parenthetical as(
		--UPDATE HERE
select 'UAL;100517' as "ticker_cik", '10_K2022_02_18_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10603322652957' as "table_name", date as "date",  ( null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) as "inventories", ( null ) as "investments", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) as "total_liabilities_and_equity", ( (  + cast( coalesce( "common_shares_outstanding_in_shares", 
0 ) as float ) ) / 1000000 ) as "common_stock_shares_outstanding" from "10_K2022_02_18_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10603322652957" union all select 'UAL;100517' as "ticker_cik", '10_K2020_02_25_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10603320647075' as "table_name", date as "date",  ( null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) as "inventories", ( null ) as "investments", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) as "total_liabilities_and_equity", ( (  + cast( coalesce( "common_shares_outstanding_in_shares", 0 ) as float ) ) / 1000000 ) as "common_stock_shares_outstanding" from "10_K2020_02_25_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10603320647075" union all select 'UAL;100517' as "ticker_cik", '10_K2018_02_22_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10603318633334' as "table_name", date as "date",  ( null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) as "inventories", ( null ) as "investments", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) as "total_liabilities_and_equity", ( (  + cast( coalesce( "common_shares_outstanding", 0 ) as float ) ) / 1000000 ) as "common_stock_shares_outstanding" from "10_K2018_02_22_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10603318633334"
	)
	,
	temp_income_statement as(
		--UPDATE HERE
select 'UAL;100517' as "ticker_cik", '10_K2022_02_18_STATEMENTS_OF_CONSOLIDATED_OPERATIONS_10603322652957' as "table_name", date as "date",  ( (  + cast( coalesce( "total_operating_revenue", 0 ) as float ) ) / 1 ) as "total_sales", ( (  - cast( coalesce( "aircraft_fuel", 0 ) as float ) - cast( coalesce( "landing_fees_and_other_rent", 0 ) as float ) - cast( coalesce( "regional_capacity_purchase", 0 ) as float ) - cast( coalesce( "aircraft_maintenance_materials_and_outside_repairs", 0 ) as float ) - 
cast( coalesce( "aircraft_rent", 0 ) as float ) ) / 1 ) as "total_cost_of_sales", ( (  - cast( coalesce( "salaries_and_related_costs", 0 ) as float ) - cast( coalesce( "distribution_expenses", 0 ) as float ) - cast( coalesce( "special_charges_credits", 0 ) as float ) - cast( coalesce( "other_operating_expenses", 0 ) as float ) ) / 1 ) as "selling_general_and_administrative_expense", ( null ) as "research_and_development", ( (  - cast( coalesce( "depreciation_and_amortization", 0 ) as float ) ) / 1 ) as "depreciation_amortisation", ( null ) as "capex", ( (  + cast( coalesce( "operating_income_loss", 0 ) as float ) ) / 1 ) as "operating_income", ( (  + cast( coalesce( "interest_expense", 0 ) as float ) + cast( coalesce( "interest_capitalized", 0 ) 
as float ) ) / 1 ) as "interest_expense", ( null ) as "interest_expense, net", ( (  - cast( coalesce( "income_tax_expense_benefit", 0 ) as float ) ) / 1 ) as "income_tax", ( (  + cast( coalesce( "net_income_loss", 0 ) as float ) ) / 1 ) as "continuing_net_earnings_total", ( null ) as "discontinued_net_earnings_total", ( (  + cast( coalesce( "net_income_loss", 0 ) as float ) ) / 1 ) as "net_earnings_to_shareholder" from "10_K2022_02_18_STATEMENTS_OF_CONSOLIDATED_OPERATIONS_10603322652957" union all select 'UAL;100517' as "ticker_cik", '10_K2020_02_25_STATEMENTS_OF_CONSOLIDATED_OPERATIONS_10603320647075' as "table_name", date as "date",  ( (  + cast( coalesce( "revenue", 0 ) as float ) ) / 1 ) as "total_sales", ( (  - cast( coalesce( "aircraft_fuel", 0 ) as float ) - cast( coalesce( "regional_capacity_purchase", 0 ) as float ) - cast( coalesce( "landing_fees_and_other_rent", 0 ) as float ) - cast( coalesce( "aircraft_maintenance_materials_and_outside_repairs", 0 ) as float ) - cast( coalesce( "aircraft_rent", 0 ) as float ) ) / 1 ) as "total_cost_of_sales", ( (  - cast( coalesce( "salaries_and_related_costs", 0 ) as float ) - cast( coalesce( "distribution_expenses", 0 ) as float ) - cast( coalesce( "special_charges", 0 ) as float ) - cast( coalesce( "other_operating_expenses", 0 ) as float ) ) / 1 ) as "selling_general_and_administrative_expense", ( null ) as "research_and_development", ( (  - cast( coalesce( "depreciation_and_amortization", 0 ) as float ) ) / 1 ) as "depreciation_amortisation", ( null ) as "capex", ( (  + cast( coalesce( "operating_income", 0 ) as float ) ) / 1 ) as "operating_income", ( (  + cast( coalesce( "interest_expense", 0 ) as float ) + cast( coalesce( "interest_capitalized", 0 ) as float ) ) / 1 ) as "interest_expense", ( null ) as "interest_expense, net", ( (  - cast( coalesce( "income_tax_expense", 0 ) as float ) ) / 1 ) as "income_tax", ( (  + cast( coalesce( "net_income", 0 ) as float ) ) / 1 ) as "continuing_net_earnings_total", ( null ) as "discontinued_net_earnings_total", ( (  + cast( coalesce( "net_income", 0 ) as float ) ) / 1 ) as "net_earnings_to_shareholder" from "10_K2020_02_25_STATEMENTS_OF_CONSOLIDATED_OPERATIONS_10603320647075" union all select 'UAL;100517' as "ticker_cik", '10_K2018_02_22_STATEMENTS_OF_CONSOLIDATED_OPERATIONS_10603318633334' as "table_name", date as "date",  ( (  + cast( coalesce( "total_operating_revenue", 0 ) as float ) ) / 1 ) as "total_sales", ( (  - cast( coalesce( "aircraft_fuel", 0 ) as float ) - cast( coalesce( "landing_fees_and_other_rent", 0 
) as float ) - cast( coalesce( "regional_capacity_purchase", 0 ) as float ) - cast( coalesce( "aircraft_maintenance_materials_and_outside_repairs", 0 ) as float ) - cast( coalesce( "aircraft_rent", 0 ) as float ) ) / 1 ) as "total_cost_of_sales", ( (  - cast( coalesce( "salaries_and_related_costs", 0 ) as float ) - cast( coalesce( "distribution_expenses", 0 ) as float ) - cast( coalesce( "special_charges_note_14", 0 ) as float ) - cast( coalesce( "other_operating_expenses", 0 ) as float ) ) / 1 ) as "selling_general_and_administrative_expense", ( null ) as "research_and_development", ( (  - cast( coalesce( "depreciation_and_amortization", 0 ) as float ) ) / 1 ) as "depreciation_amortisation", ( null ) as "capex", ( (  + cast( coalesce( "operating_income", 0 ) as float ) ) 
/ 1 ) as "operating_income", ( (  + cast( coalesce( "interest_expense", 0 ) as float ) + cast( coalesce( "interest_capitalized", 0 ) as float ) ) / 1 ) as "interest_expense", ( null ) as "interest_expense, net", ( (  - cast( coalesce( "income_tax_expense_benefit_note_14", 0 ) as float ) ) / 1 ) as "income_tax", ( (  + cast( coalesce( "net_income", 0 ) as float ) ) / 1 ) as "continuing_net_earnings_total", ( null ) as "discontinued_net_earnings_total", ( (  + cast( coalesce( "net_income", 0 ) as float ) ) / 1 ) as "net_earnings_to_shareholder" from "10_K2018_02_22_STATEMENTS_OF_CONSOLIDATED_OPERATIONS_10603318633334"
	)
	,
	--TRANSFORMATION DONE FROM THIS POINT ONWARDS
	temp_bs_parent as(
		select
			 e1.*
		from(
				select
					 w1.ticker_cik
					, w1.date
					, w1.common_stock_shares_outstanding as "common_stock_outstanding"
					, row_number( ) over(
					  	 partition by
					  	 	 w1.date
					  	 order by
					  	 	 w1.common_stock_shares_outstanding
					  ) as "ranking"
				from
					temp_balance_sheet_parenthetical w1
		) e1
		where
			 e1.ranking = 1
	)
	,
	temp_is as(
		select
			 e1.*
			, e2.common_stock_outstanding
		from(
				select
					 w1.*
					, row_number( ) over(
					  	 partition by
					  	 	 w1.date
					  	 order by
					  	 	 w1.net_earnings_to_shareholder
					  ) as "ranking"
				from
					temp_income_statement w1
		) 					e1
		left join
			temp_bs_parent 	e2 on
				 e1.ticker_cik = e2.ticker_cik
				 	and
				 e1.date = e2.date
		where
			 e1.ranking = 1
	)
	,
	temp_bs as(
		select
			 e1.*
		from(
				select
					 w1.*
					, row_number( ) over(
					  	 partition by
					  	 	 w1.date
					  	 order by
					  	 	 w1.total_shareholder_equity
					  ) as "ranking"
				from
					temp_balance_sheet w1
		) e1
		where
			 e1.ranking = 1
	)
select
	 w1.ticker_cik
	, w1.table_name
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
	, w2.total_current_assets
	, w2.total_current_liabilities
	, w2.total_current_assets - w2.total_current_liabilities as "working_capital"
	, round(
	  	 cast( w2.total_current_assets - w2.total_current_liabilities as float )
	  	 / w1.common_stock_outstanding
	  	, 2
	  ) as "wc_per_share"
	, round( cast( w1.net_earnings_to_shareholder as float ) / w2.total_shareholder_equity * 100, 2 ) as "roe"
	, round( cast( w1.net_earnings_to_shareholder as float ) / w1.total_sales * 100, 2 ) as "earnings_margin_pct"
	, round( cast( w1.total_sales as float ) / w2.total_assets, 2 ) as "asset_turnover"
	, round( cast( w2.total_assets as float ) / w2.total_shareholder_equity, 2 ) as "equity_multiplier"
from
	temp_is w1
left join
	temp_bs w2 on
		 w1.ticker_cik = w2.ticker_cik
		 	and
		 w1.date = w2.date
order by
	 w1.date
;
select
	 w1.*
from
	temp_bs w1
;

	
	
	
	
	
--select
--	 w1.*
--from
--	temp_balance_sheet w1
--;
--select
--	 w1.*
--from
--	temp_balance_sheet_parenthetical w1
--;
select
	 w1.*
from
	temp_income_statement w1
;