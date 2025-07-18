-- Project Description:
-- The project aims to create a single table with behavioural indicators, 
-- to be used as the basis for a machine learning model to predict the future behaviour of a bank's customers. 
-- The table will collect transactional data and customer account information.


-- Creation of temporary table ‘eta'
-- This table calculates the age of each customer based on the date of birth
create temporary table banca.eta as(
select 
	cli.id_cliente, 
	timestampdiff(year, data_nascita, CURDATE()) as eta
from banca.cliente as cli
group by 1, 2
);


-- Creation of the temporary table ‘trans_uscita_entrata'
-- This table aggregates the number and amount of incoming and outgoing transactions for each customer
create temporary table banca.trans_uscita_entrata as (
select
	conto.id_cliente, 
    sum(case when trans.id_tipo_trans in (0, 1, 2) then 1 else 0 end) as numero_transazioni_in_entrata,
    sum(case when trans.id_tipo_trans in (3, 4, 5, 6, 7) then 1 else 0 end) as numero_transazioni_in_uscita,
	round(sum(case when trans.id_tipo_trans in (0,1,2) then trans.importo else 0 end),2) as importo_transazioni_in_entrata,
	round(sum(case when trans.id_tipo_trans in (3,4,5,6,7) then trans.importo else 0 end),2) as importo_transazioni_in_uscita
from banca.conto conto
left join banca.transazioni trans
on trans.id_conto = conto.id_conto
group by 1
);


-- Creation of temporary table ‘tipo_conto_cliente'
-- This table aggregates the number of accounts and the account type for each customer
create temporary table banca.tipo_conto_cliente as (
select
	conto.id_cliente, 
	count(distinct conto.id_conto) as numero_di_conti,
	sum(case when conto.id_tipo_conto = 0 then 1 else 0 end) as tipo_conto_base,
	sum(case when conto.id_tipo_conto = 1 then 1 else 0 end) as tipo_conto_business,
	sum(case when conto.id_tipo_conto = 2 then 1 else 0 end) as tipo_conto_privati,
	sum(case when conto.id_tipo_conto = 3 then 1 else 0 end) as tipo_conto_famiglie
from banca.conto conto
group by 1
);


-- Creation of temporary table ‘sub_tipo_trans'
-- This table aggregates the number of transactions by transaction type for each customer
create temporary table banca.sub_tipo_trans as (
select
	conto.id_cliente, 
	sum(case when trans.id_tipo_trans = 0 then 1 else 0 end) as trans_entrata_Stipendio,
	sum(case when trans.id_tipo_trans = 1 then 1 else 0 end) as trans_entrata_Pensione,
	sum(case when trans.id_tipo_trans = 2 then 1 else 0 end) as trans_entrata_Dividendi,
	sum(case when trans.id_tipo_trans = 3 then 1 else 0 end) as trans_uscita_Acquisto_su_Amazon,
	sum(case when trans.id_tipo_trans = 4 then 1 else 0 end) as trans_uscita_Rata_mutuo,
	sum(case when trans.id_tipo_trans = 5 then 1 else 0 end) as trans_uscita_Hotel,
	sum(case when trans.id_tipo_trans = 6 then 1 else 0 end) as trans_uscita_Biglietto_aereo,
	sum(case when trans.id_tipo_trans = 7 then 1 else 0 end) as trans_uscita_Supermercato
from banca.conto conto
left join banca.transazioni trans
on trans.id_conto = conto.id_conto
group by 1
);


-- Creation of the temporary table  'importo_trans'
-- This table aggregates the amount of incoming and outgoing transactions by account type for each customer
create temporary table banca.importo_tipo_conto as (
select
	conto.id_cliente, 
	round(sum(case when conto.id_tipo_conto = 0 and trans.id_tipo_trans in (0,1,2) then trans.importo else 0 end),2) as importo_entrata_ContoBase,
	round(sum(case when conto.id_tipo_conto = 0 and trans.id_tipo_trans in (3,4,5,6,7) then trans.importo else 0 end),2) as importo_uscita_ContoBase,
	round(sum(case when conto.id_tipo_conto = 1 and trans.id_tipo_trans in (0,1,2) then trans.importo else 0 end),2) as importo_entrata_ContoBusiness,
	round(sum(case when conto.id_tipo_conto = 1 and trans.id_tipo_trans in (3,4,5,6,7) then trans.importo else 0 end),2) as importo_uscita_ContoBusiness,
	round(sum(case when conto.id_tipo_conto = 2 and trans.id_tipo_trans in (0,1,2) then trans.importo else 0 end),2) as importo_entrata_ContoPrivati,
	round(sum(case when conto.id_tipo_conto = 2 and trans.id_tipo_trans in (3,4,5,6,7) then trans.importo else 0 end),2) as importo_uscita_ContoPrivati,
	round(sum(case when conto.id_tipo_conto = 3 and trans.id_tipo_trans in (0,1,2) then trans.importo else 0 end),2) as importo_entrata_ContoFamiglie,
	round(sum(case when conto.id_tipo_conto = 3 and trans.id_tipo_trans in (3,4,5,6,7) then trans.importo else 0 end),2) as importo_uscita_ContoFamiglie
from banca.conto conto
left join banca.transazioni trans
on trans.id_conto = conto.id_conto
group by 1
);


-- Creation of the temporary table 'total'
-- This table combines all the information gathered in the previous temporary tables
create temporary table banca.total as (
select 
	eta.id_cliente as id_cliente,
	eta.eta,
	coalesce(tra.numero_transazioni_in_uscita, 0) as numero_transazioni_in_uscita,
	coalesce(tra.numero_transazioni_in_entrata, 0) as numero_transazioni_in_entrata,
	coalesce(tra.importo_transazioni_in_uscita, 0) as importo_transazioni_in_uscita,
	coalesce(tra.importo_transazioni_in_entrata, 0) as importo_transazioni_in_entrata,
	coalesce(coc.numero_di_conti, 0) as numero_di_conti,
	coalesce(coc.tipo_conto_base, 0) as tipo_conto_base,
	coalesce(coc.tipo_conto_business, 0) as tipo_conto_business,
	coalesce(coc.tipo_conto_privati, 0) as tipo_conto_privati,
	coalesce(coc.tipo_conto_famiglie, 0) as tipo_conto_famiglie,
	coalesce(sub.trans_uscita_Acquisto_su_Amazon, 0) as trans_uscita_Acquisto_su_Amazon,
	coalesce(sub.trans_uscita_Rata_mutuo, 0) as trans_uscita_Rata_mutuo,
	coalesce(sub.trans_uscita_Hotel, 0) as trans_uscita_Hotel,
	coalesce(sub.trans_uscita_Biglietto_aereo, 0) as trans_uscita_Biglietto_aereo,
	coalesce(sub.trans_uscita_Supermercato, 0) as trans_uscita_Supermercato,
	coalesce(sub.trans_entrata_Stipendio, 0) as trans_entrata_Stipendio,
	coalesce(sub.trans_entrata_Pensione, 0) as trans_entrata_Pensione,
	coalesce(sub.trans_entrata_Dividendi, 0) as trans_entrata_Dividendi,
	coalesce(imp.importo_entrata_ContoBase, 0) as importo_entrata_ContoBase,
	coalesce(imp.importo_uscita_ContoBase, 0) as importo_uscita_ContoBase,
	coalesce(imp.importo_entrata_ContoBusiness, 0) as importo_entrata_ContoBusiness,
	coalesce(imp.importo_uscita_ContoBusiness, 0) as importo_uscita_ContoBusiness,
	coalesce(imp.importo_entrata_ContoPrivati, 0) as importo_entrata_ContoPrivati,
	coalesce(imp.importo_uscita_ContoPrivati, 0) as importo_uscita_ContoPrivati,
	coalesce(imp.importo_entrata_ContoFamiglie, 0) as importo_entrata_ContoFamiglie,
	coalesce(imp.importo_uscita_ContoFamiglie, 0) as importo_uscita_ContoFamiglie
from banca.eta eta
left join banca.trans_uscita_entrata tra on eta.id_cliente = tra.id_cliente
left join banca.tipo_conto_cliente coc on eta.id_cliente = coc.id_cliente
left join banca.sub_tipo_trans sub on eta.id_cliente = sub.id_cliente
left join banca.importo_tipo_conto imp on eta.id_cliente = imp.id_cliente
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27
order by 1
);


select * from banca.total;