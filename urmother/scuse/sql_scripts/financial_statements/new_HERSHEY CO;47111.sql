
--=====================================================
--	COMPANY NAME : HERSHEY CO
--	CENTRAL INDEX KEY : 47111
--=====================================================

with
	temp_balance_sheet as(
		--UPDATE HERE
		select 'HSY;47111' as "ticker_cik", date as "date",  ( coalesce( cast(  + "cash_and_cash_equivalents" as float ), 0 ) / 1000 ) as "cash_and_equivalents", ( coalesce( cast(  + "accounts_receivable_trade_net" as float ), 0 ) / 1000 ) as "accounts_receivable_net", ( coalesce( cast(  + "inventories" as float ), 0 ) / 1000 ) as "inventories", ( coalesce( cast(  + "total_current_assets" as float ), 0 ) / 1000 ) as "total_current_assets", ( coalesce( cast(  + "property_plant_and_equipment_net" as float ), 0 ) / 1000 ) as "property_plant_and_equipment_net", ( coalesce( cast(  + "goodwill" as float ), 0 ) / 1000 ) as "goodwill", ( coalesce( cast(  + "other_intangibles" as float ), 0 ) / 1000 ) as "intangible_assets", ( coalesce( cast(  + "total_assets" as float ), 0 ) / 1000 ) as "total_assets", ( coalesce( cast(  + "short_term_debt" + "current_portion_of_long_term_debt" as float ), 0 ) / 1000 ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( coalesce( cast(  + "accounts_payable" as float ), 0 ) / 1000 ) as "accounts_payable", ( coalesce( cast(  + "total_current_liabilities" as float ), 0 ) / 1000 ) as "total_current_liabilities", 
( null ) as "long_term_unearned_revenue", ( coalesce( cast(  + "long_term_debt" as float ), 0 ) / 1000 ) as "long_term_debt", ( null ) as "total_debt", ( coalesce( cast(  + "total_liabilities" as float ), 0 ) / 1000 ) as "total_liabilities", ( coalesce( cast(  + "additional_paid_in_capital" as float ), 0 ) / 1000 ) as "total_paid_in_capital", ( coalesce( cast(  + "treasury_common_stock_shares_at_cost_15444011_in_2021_and_13325898_in_2020" as float ), 0 ) / 1000 ) as "treasury_stock", ( coalesce( cast(  + "retained_earnings" as float ), 0 ) / 1000 ) as "retained_earnings", ( coalesce( cast(  + "total_the_hershey_company_stockholders_equity" as float ), 0 ) / 1000 ) as "total_shareholder_equity", ( coalesce( cast(  + "noncontrolling_interest_in_subsidiary" as float ), 0 ) / 1000 ) as "non_controlling_interest", ( coalesce( cast(  + "total_liabilities_and_stockholders_equity" as float ), 0 ) / 1000 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2022_02_18_CONSOLIDATED_BALANCE_SHEETS_10018322652624" union all select 'HSY;47111' as "ticker_cik", date as "date",  ( coalesce( cast(  + "cash_and_cash_equivalents" as float ), 0 ) / 1000 ) as "cash_and_equivalents", ( coalesce( cast(  + "accounts_receivable_trade_net" as float ), 0 ) / 1000 ) as "accounts_receivable_net", ( coalesce( cast(  + "inventories" as float ), 0 ) / 1000 ) as 
"inventories", ( coalesce( cast(  + "total_current_assets" as float ), 0 ) / 1000 ) as "total_current_assets", ( coalesce( cast(  + "property_plant_and_equipment_net" as float ), 0 ) / 1000 ) as "property_plant_and_equipment_net", ( coalesce( cast(  + "goodwill" as float ), 0 ) / 1000 ) as "goodwill", ( coalesce( cast(  + "other_intangibles" as float ), 0 ) / 1000 ) as "intangible_assets", ( coalesce( cast(  + "total_assets" as float ), 0 ) / 1000 ) as "total_assets", ( coalesce( cast(  + "short_term_debt" + "current_portion_of_long_term_debt" as float ), 0 ) / 1000 ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( coalesce( cast(  + "accounts_payable" as float ), 0 ) 
/ 1000 ) as "accounts_payable", ( coalesce( cast(  + "total_current_liabilities" as float ), 0 ) / 1000 ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( coalesce( cast(  + "long_term_debt" as float ), 0 
) / 1000 ) as "long_term_debt", ( null ) as "total_debt", ( coalesce( cast(  + "total_liabilities" as float ), 0 ) / 1000 ) as "total_liabilities", ( coalesce( cast(  + "additional_paid_in_capital" as float ), 0 ) / 1000 ) as "total_paid_in_capital", ( coalesce( cast(  + "treasury_common_stock_shares_at_cost_12723592_in_2019_and_150172840_in_2018" as float ), 0 ) / 1000 ) as "treasury_stock", ( coalesce( cast(  + "retained_earnings" as float ), 0 ) / 1000 ) as "retained_earnings", ( coalesce( cast(  + "total_the_hershey_company_stockholders_equity" as float ), 0 ) / 1000 ) as "total_shareholder_equity", ( coalesce( cast(  + "noncontrolling_interest_in_subsidiary" as float ), 0 ) / 1000 ) as "non_controlling_interest", ( coalesce( cast(  + "total_liabilities_and_stockholders_equity" as float ), 0 ) / 1000 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2020_02_20_CONSOLIDATED_BALANCE_SHEETS_10018320635096" union all select 'HSY;47111' as "ticker_cik", date as "date",  ( coalesce( cast(  + "cash_and_cash_equivalents" as float ), 0 ) / 1000 ) as "cash_and_equivalents", ( coalesce( cast(  + "accounts_receivable_trade_net" as float ), 0 ) / 1000 ) as "accounts_receivable_net", ( coalesce( cast(  + "inventories" as float ), 0 ) / 1000 ) as "inventories", ( coalesce( cast(  + "total_current_assets" as float ), 0 ) / 1000 ) as "total_current_assets", ( coalesce( cast(  + "property_plant_and_equipment_net" as float ), 0 ) / 1000 ) as "property_plant_and_equipment_net", ( coalesce( cast(  + "goodwill" as float ), 0 ) / 1000 ) as "goodwill", ( coalesce( cast(  + "other_intangibles" as float ), 0 ) / 1000 ) as "intangible_assets", ( coalesce( cast(  + "total_assets" as float ), 0 ) / 1000 ) as "total_assets", ( coalesce( cast(  + "short_term_debt" + "current_portion_of_long_term_debt" as float ), 0 ) / 1000 ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( coalesce( cast(  + "accounts_payable" as float ), 0 ) / 1000 ) as "accounts_payable", ( coalesce( cast(  + "total_current_liabilities" as float ), 
0 ) / 1000 ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( coalesce( cast(  + "long_term_debt" as float ), 0 ) / 1000 ) as "long_term_debt", ( null ) as "total_debt", ( coalesce( cast(  + "total_liabilities" as float ), 0 ) / 1000 ) as "total_liabilities", ( coalesce( cast(  + "additional_paid_in_capital" as float ), 0 ) / 1000 ) as "total_paid_in_capital", ( coalesce( cast(  + "treasury_common_stock_shares_at_cost_149040927_in_2017_and_147642009_in_2016" as float ), 0 ) / 1000 ) as "treasury_stock", ( coalesce( cast(  + "retained_earnings" as float ), 0 ) / 1000 ) as "retained_earnings", ( coalesce( cast(  + "total_the_hershey_company_stockholders_equity" as float ), 0 ) / 1000 ) as "total_shareholder_equity", ( coalesce( cast(  + "noncontrolling_interest_in_subsidiary" as float ), 0 ) / 1000 ) as "non_controlling_interest", ( coalesce( cast(  + "total_liabilities_and_stockholders_equity" as float ), 0 ) / 1000 ) as "total_liabilities_and_equity", ( null ) as "common_stock_shares_outstanding" from "10_K2018_02_27_CONSOLIDATED_BALANCE_SHEETS_10018318643802"
	)
	,
	temp_balance_sheet_parenthetical as(
		--UPDATE HERE
		select 'HSY;47111' as "ticker_cik", date as "date",  ( null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) as "inventories", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) as "total_liabilities_and_equity", ( coalesce( cast(  - "treasury_stock_shares_shares" + "common_stock_shares_issued_shares.1" + "common_stock_shares_issued_shares.2" as float ), 0 ) / 1000000 ) as "common_stock_shares_outstanding" from "10_K2022_02_18_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10018322652624" union all select 'HSY;47111' as "ticker_cik", date as "date",  ( null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) as "inventories", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) as "total_liabilities_and_equity", ( coalesce( cast(  - "treasury_stock_shares_shares" + "common_stock_shares_issued_shares.1" + "common_stock_shares_issued_shares.2" as float ), 0 ) / 1000000 ) as "common_stock_shares_outstanding" from "10_K2020_02_20_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10018320635096" union all select 'HSY;47111' as "ticker_cik", date as "date",  ( null ) as "cash_and_equivalents", ( null ) as "accounts_receivable_net", ( null ) as "inventories", ( null ) as "total_current_assets", ( null ) as "property_plant_and_equipment_net", ( null ) as "goodwill", ( null ) as "intangible_assets", ( null ) as "total_assets", ( null ) as "short_term_debt", ( null ) as "short_term_unearned_revenue", ( null ) as "accounts_payable", ( null ) as "total_current_liabilities", ( null ) as "long_term_unearned_revenue", ( null ) as "long_term_debt", ( null ) as "total_debt", ( null ) as "total_liabilities", ( null ) as "total_paid_in_capital", ( null ) as "treasury_stock", ( null ) as "retained_earnings", ( null ) as "total_shareholder_equity", ( null ) as "non_controlling_interest", ( null ) 
as "total_liabilities_and_equity", ( coalesce( cast(  - "treasury_stock_shares_shares" + "common_stock_shares_issued_shares.1" + "common_stock_shares_issued_shares.2" as float ), 0 ) / 1000000 ) as "common_stock_shares_outstanding" 
from "10_K2018_02_27_CONSOLIDATED_BALANCE_SHEETS_PARENTHETICAL_10018318643802"
	)
	,
	temp_income_statement as(
		--UPDATE HERE
		select 'HSY;47111' as "ticker_cik", date as "date",  ( coalesce( cast(  + "net_sales" as float ), 0 ) / 1000 ) as "total_sales", ( coalesce( cast(  - "cost_of_sales" as float ), 0 ) / 1000 ) as "total_cost_of_sales", ( coalesce( cast(  - "selling_marketing_and_administrative_expense" as float ), 0 ) / 1000 ) as "selling_general_and_administrative_expense", ( null ) as "research_and_development", ( null ) as "depreciation_amortisation", ( null ) as "capex", ( 
coalesce( cast(  + "operating_profit" as float ), 0 ) / 1000 ) as "operating_income", ( null ) as "interest_expense", ( coalesce( cast(  - "interest_expense_net" as float ), 0 ) / 1000 ) as "interest_expense, net", ( coalesce( cast(  - "provision_for_income_taxes" as float ), 0 ) / 1000 ) as "income_tax", ( coalesce( cast(  + "net_income_including_noncontrolling_interest" as float ), 0 ) / 1000 ) as "continuing_net_earnings_total", ( null ) as "discontinued_net_earnings_total", ( coalesce( cast(  + "net_income_attributable_to_the_hershey_company" as float ), 0 ) / 1000 ) as "net_earnings_to_shareholder" from "10_K2022_02_18_CONSOLIDATED_STATEMENTS_OF_INCOME_10018322652624" union all select 'HSY;47111' as "ticker_cik", date as "date",  ( coalesce( cast(  + "net_sales" as float ), 0 ) / 1000 ) as "total_sales", ( coalesce( cast(  - "cost_of_sales" as float ), 0 ) / 1000 ) as "total_cost_of_sales", ( coalesce( cast(  
- "selling_marketing_and_administrative_expense" as float ), 0 ) / 1000 ) as "selling_general_and_administrative_expense", ( null ) as "research_and_development", ( null ) as "depreciation_amortisation", ( null ) as "capex", ( coalesce( cast(  + "operating_profit" as float ), 0 ) / 1000 ) as "operating_income", ( null ) as "interest_expense", ( coalesce( cast(  - "interest_expense_net" as float ), 0 ) / 1000 ) as "interest_expense, net", ( coalesce( cast(  - "provision_for_income_taxes" as float ), 0 ) / 1000 ) as "income_tax", ( coalesce( cast(  + "net_income_including_noncontrolling_interest" as float ), 0 ) / 1000 ) as "continuing_net_earnings_total", ( null ) as "discontinued_net_earnings_total", ( coalesce( cast(  + "net_income_attributable_to_the_hershey_company" as float ), 0 ) / 1000 ) as "net_earnings_to_shareholder" from "10_K2020_02_20_CONSOLIDATED_STATEMENTS_OF_INCOME_10018320635096" union all select 'HSY;47111' as "ticker_cik", date as "date",  ( coalesce( cast(  + "net_sales" as float ), 0 ) / 1000 ) as "total_sales", ( coalesce( cast(  - "cost_of_sales" as float ), 0 ) / 1000 ) as "total_cost_of_sales", ( coalesce( cast(  - "selling_marketing_and_administrative_expense" as float ), 0 ) / 1000 ) as "selling_general_and_administrative_expense", ( null ) as "research_and_development", ( null ) as "depreciation_amortisation", ( null ) as "capex", ( coalesce( 
cast(  + "operating_profit" as float ), 0 ) / 1000 ) as "operating_income", ( null ) as "interest_expense", ( coalesce( cast(  - "interest_expense_net" as float ), 0 ) / 1000 ) as "interest_expense, net", ( coalesce( cast(  - "provision_for_income_taxes" as float ), 0 ) / 1000 ) as "income_tax", ( coalesce( cast(  + "net_income_including_noncontrolling_interest" as float ), 0 ) / 1000 ) as "continuing_net_earnings_total", ( null ) as "discontinued_net_earnings_total", ( coalesce( cast(  + "net_income_attributable_to_the_hershey_company" as float ), 0 ) / 1000 ) as "net_earnings_to_shareholder" from "10_K2018_02_27_CONSOLIDATED_STATEMENTS_OF_INCOME_10018318643802"
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
select * from temp_balance_sheet
;
select
	 w1.ticker_cik
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