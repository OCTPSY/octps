
--=====================================================
--	COMPANY NAME : PALANTIR TECHNOLOGIES INC.
--	CENTRAL INDEX KEY : 1321655
--=====================================================

with
	temp_balance_sheet as(
		--UPDATE HERE
		select 'PLTR;1321655' as "ticker_cik", '10_K2022_02_24_CONSOLIDATED_BALANCE_SHEETS_13954022666360' as "table_name", date as "date",  ( (  + cast( coalesce( "cash_and_cash_equivalents", 0 ) as float ) + cast( coalesce( "restricted_cash", 0 ) as float ) + cast( coalesce( "restricted_cash_noncurrent", 0 ) as float ) ) / 1000 ) as "cash_and_equivalents", ( (  + cast( coalesce( "accounts_receivable", 0 ) as float ) ) / 1000 ) as "accounts_receivable_net", ( null ) as "inventories", ( (  + cast( coalesce( "marketable_securities", 0 ) as float ) ) / 1000 ) as "investments", ( (  + cast( coalesce( "total_current_assets", 0 ) as float ) ) / 1000 ) as "total_current_assets", ( (  + cast( coalesce( "property_and_equipment_net", 0 ) as float ) + cast( coalesce( "operating_lease right_of_use assets", 0 ) as float ) ) / 1000 ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( (  + cast( coalesce( "total_assets", 0 ) as float ) ) / 1000 ) as "total_assets", ( (  + cast( coalesce( "operating_lease_liabilities", 0 ) as float ) ) / 1000 ) as "short_term_debt", ( (  + cast( coalesce( "deferred_revenue", 0 ) as float ) + cast( coalesce( "customer_deposits", 0 ) as float ) ) / 1000 ) as "short_term_unearned_revenue", ( (  + cast( coalesce( "accounts_payable", 0 ) as float ) ) / 1000 ) as "accounts_payable", ( (  + cast( coalesce( "total_current_liabilities", 0 ) as float ) ) / 1000 ) as "total_current_liabilities", ( (  + cast( coalesce( "deferred_revenue_noncurrent", 0 ) as float ) + cast( coalesce( "customer_deposits_noncurrent", 0 ) as float ) ) / 1000 ) as "long_term_unearned_revenue", ( (  + cast( coalesce( "debt_noncurrent_net", 0 ) as float ) + cast( coalesce( "operating_lease_liabilities_noncurrent", 0 ) as float ) ) / 1000 ) as "long_term_debt", ( (  + cast( coalesce( "operating_lease_liabilities", 0 ) as float ) + cast( coalesce( "debt_noncurrent_net", 0 ) as float ) + cast( coalesce( "operating_lease_liabilities_noncurrent", 0 ) as float ) ) / 1000 ) as "total_debt", ( (  + cast( coalesce( "total_liabilities", 0 ) as float ) ) / 1000 ) as "total_liabilities", ( (  + cast( coalesce( "common_stock", 0 ) as float ) + cast( coalesce( "additional_paid_in_capital", 0 ) as float ) ) / 1000 ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( (  + cast( coalesce( "accumulated_deficit", 0 ) as float ) ) / 1000 ) as "retained_earnings", ( (  + cast( coalesce( "total_stockholders_equity", 0 ) as float ) ) / 1000 ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( (  + cast( coalesce( "total_liabilities_and_stockholders_equity", 0 ) as float ) ) / 1000 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2022_02_24_CONSOLIDATED_BALANCE_SHEETS_13954022666360" union all select 'PLTR;1321655' as "ticker_cik", '10_K2021_02_26_CONSOLIDATED_BALANCE_SHEETS_13954021689968' as "table_name", date as "date",  ( (  + cast( coalesce( "cash_and_cash_equivalents", 0 ) as float ) 
+ cast( coalesce( "restricted_cash", 0 ) as float ) + cast( coalesce( "restricted_cash_noncurrent", 0 ) as float ) ) / 1000 ) as "cash_and_equivalents", ( (  + cast( coalesce( "accounts_receivable", 0 ) as float ) ) / 1000 ) as "accounts_receivable_net", ( null ) as "inventories", ( null ) as "investments", ( (  + cast( coalesce( "total_current_assets", 0 ) as float ) ) / 1000 ) as "total_current_assets", ( (  + cast( coalesce( "property_and_equipment_net", 0 
) as float ) + cast( coalesce( "operating_lease right_of_use assets", 0 ) as float ) ) / 1000 ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( (  + cast( coalesce( "total_assets", 0 ) as float ) ) / 1000 ) as "total_assets", ( (  + cast( coalesce( "operating_lease_liabilities", 0 ) as float ) ) / 1000 ) as "short_term_debt", ( (  + cast( coalesce( "deferred_revenue", 0 ) as float ) + cast( coalesce( "customer_deposits", 0 ) as float ) ) / 1000 ) as "short_term_unearned_revenue", ( (  + cast( coalesce( "accounts_payable", 0 ) as float ) ) / 1000 ) as "accounts_payable", ( (  + cast( coalesce( "total_current_liabilities", 0 ) as float ) ) 
/ 1000 ) as "total_current_liabilities", ( (  + cast( coalesce( "deferred_revenue_noncurrent", 0 ) as float ) + cast( coalesce( "customer_deposits_noncurrent", 0 ) as float ) ) / 1000 ) as "long_term_unearned_revenue", ( (  + cast( 
coalesce( "debt_noncurrent_net", 0 ) as float ) + cast( coalesce( "operating_lease_liabilities_noncurrent", 0 ) as float ) ) / 1000 ) as "long_term_debt", ( (  + cast( coalesce( "operating_lease_liabilities", 0 ) as float ) + cast( 
coalesce( "debt_noncurrent_net", 0 ) as float ) + cast( coalesce( "operating_lease_liabilities_noncurrent", 0 ) as float ) ) / 1000 ) as "total_debt", ( (  + cast( coalesce( "total_liabilities", 0 ) as float ) ) / 1000 ) as "total_liabilities", ( (  + cast( coalesce( "additional_paid_in_capital", 0 ) as float ) ) / 1000 ) as "total_paid_in_capital", ( (  + cast( coalesce( "treasury_stock", 0 ) as float ) ) / 1000 ) as "treasury_stock", ( (  + cast( coalesce( "accumulated_deficit", 0 ) as float ) ) / 1000 ) as "retained_earnings", ( (  + cast( coalesce( "common_stock", 0 ) as float ) + cast( coalesce( "total_stockholders_equity_deficit", 0 ) as float ) ) / 1000 ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( (  + cast( coalesce( "total_liabilities_redeemable_convertible_and_convertible_preferred_stock_and_stockholders_equity_deficit", 0 ) as float ) ) / 1000 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2021_02_26_CONSOLIDATED_BALANCE_SHEETS_13954021689968"
	)
	,
	temp_balance_sheet_parenthetical as(
		--UPDATE HERE
		select 'PLTR;1321655' as "ticker_cik", '10_K2022_02_24_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_13954022666360' as "table_name", date as "date",  ( null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) 
as "inventories", ( null ) as "investments", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) as "total_liabilities_and_equity", ( (  + cast( coalesce( "common_stock_shares_outstanding", 0 ) as float ) ) / 1000000 ) as "common_stock_shares_outstanding" from "10_K2022_02_24_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_13954022666360" union all select 'PLTR;1321655' as "ticker_cik", '10_K2021_02_26_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_13954021689968' as "table_name", date as "date",  ( null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) 
as "inventories", ( null ) as "investments", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) as "total_liabilities_and_equity", ( (  + cast( coalesce( "common_stock_shares_outstanding", 0 ) as float ) - cast( coalesce( "treasury_stock_shares", 0 ) as float ) ) / 1000000 ) as "common_stock_shares_outstanding" from "10_K2021_02_26_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_13954021689968"
	)
	,
	temp_income_statement as(
		--UPDATE HERE
		select 'PLTR;1321655' as "ticker_cik", '10_K2022_02_24_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13954022666360' as "table_name", date as "date",  ( (  + cast( coalesce( "revenue", 0 ) as float ) ) / 1000 ) as "total_sales", ( (  - cast( coalesce( "cost_of_revenue", 0 ) as float ) ) / 1000 ) as "total_cost_of_sales", ( (  - cast( coalesce( "sales_and_marketing", 0 ) as float ) - cast( coalesce( "general_and_administrative", 0 ) as float ) ) / 1000 ) as "selling_general_and_administrative_expense", ( (  - cast( coalesce( "research_and_development", 0 ) as float ) ) / 1000 ) as "research_and_development", ( null ) as "depreciation_amortisation", ( null ) as "capex", ( (  + cast( coalesce( "loss_from_operations", 0 ) as float ) ) / 1000 ) as "operating_income", ( (  + cast( coalesce( "interest_expense", 0 ) as float ) ) / 1000 ) as "interest_expense", ( null ) as "interest_expense, net", ( (  - cast( coalesce( "provision_for_benefit_from_income_taxes", 0 ) as float ) ) / 1000 ) as "income_tax", ( (  + cast( coalesce( "net_loss", 0 ) as float ) ) / 1000 ) as "continuing_net_earnings_total", ( null ) as "discontinued_net_earnings_total", ( (  + cast( coalesce( "net_loss", 0 ) as float ) ) / 1000 ) as "net_earnings_to_shareholder" from "10_K2022_02_24_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13954022666360" union all select 'PLTR;1321655' as "ticker_cik", '10_K2021_02_26_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13954021689968' as "table_name", date as "date",  ( (  + cast( coalesce( "revenue", 0 ) as float ) ) / 1000 ) as "total_sales", ( (  - cast( coalesce( "cost_of_revenue", 0 ) as float ) ) / 1000 ) as "total_cost_of_sales", ( (  - cast( coalesce( "sales_and_marketing", 0 ) as float ) - cast( coalesce( "general_and_administrative", 0 ) as float ) ) / 1000 ) as "selling_general_and_administrative_expense", ( (  - cast( coalesce( "research_and_development", 0 ) as float ) ) / 1000 ) as "research_and_development", ( null ) as "depreciation_amortisation", ( null ) as "capex", ( (  + cast( coalesce( "loss_from_operations", 0 ) as float ) ) / 1000 ) as "operating_income", ( (  + cast( coalesce( "interest_expense", 0 ) as float ) ) / 1000 ) as "interest_expense", ( null ) as "interest_expense, net", ( (  - cast( coalesce( "provision_benefit_for_income_taxes", 0 ) as float ) ) / 1000 ) as "income_tax", ( (  + cast( coalesce( "net_loss", 0 ) as float ) ) / 1000 ) as "continuing_net_earnings_total", ( null ) as "discontinued_net_earnings_total", ( (  + cast( coalesce( "net_loss_attributable_to_common_stockholders", 0 ) as float ) ) / 1000 ) as "net_earnings_to_shareholder" from "10_K2021_02_26_CONSOLIDATED_STATEMENTS_OF_OPERATIONS_13954021689968"
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