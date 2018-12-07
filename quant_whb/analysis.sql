--profit sql
select year(state_dt) 年度 ,sum(profit) 收益 from my_capital  group by  year(state_dt)
select year(state_dt) 年度,month(state_dt) 月份,sum(profit) 收益 from my_capital  group by  year(state_dt),month(state_dt)
select bz 操作类型,sum(profit) 收益 from my_capital group by  bz -- group by operate type 
select year(state_dt) 年度,stock_code,sum(profit) 收益 from my_capital group by  year(state_dt)  ,stock_code;

select * from my_capital where capital>=(select max(capital) from my_capital)
union all 
select * from my_capital where capital<=(select min(capital) from my_capital)


--连续下跌
set @a:=1,@b:=1,@c:=0;
select cum_lose_day,count(1) days from 
(
select state_dt,case when rise=1 then @c:=@c+rise else @c:=0 end as  cum_lose_day  from 
(select a.state_dt,b.state_dt dt,a.capital,b.capital cp,case when b.capital-a.capital >0 then 0 else 1 end rise from 
(select a.*,@a:=@a+1 id from my_capital a join  (select max(seq) seq,state_dt from my_capital group by state_dt) b  on a.seq=b.seq order by state_dt asc ) a 
join 
(select a.*,@b:=@b+1 id  from my_capital a join  (select max(seq) seq,state_dt from my_capital group by state_dt) b  on a.seq=b.seq order by state_dt asc) b 
 where a.id-b.id=-1) c) d  group by cum_lose_day;

--连续上涨 
set @a:=1,@b:=1,@c:=0;
select cum_rise_day,count(1) days from 
(
select state_dt,case when rise=0 then @c:=@c+1 else @c:=0 end as  cum_rise_day  from 
(select a.state_dt,b.state_dt dt,a.capital,b.capital cp,case when b.capital-a.capital >0 then 0 else 1 end rise from 
(select a.*,@a:=@a+1 id from my_capital a join  (select max(seq) seq,state_dt from my_capital group by state_dt) b  on a.seq=b.seq order by state_dt asc ) a 
join 
(select a.*,@b:=@b+1 id  from my_capital a join  (select max(seq) seq,state_dt from my_capital group by state_dt) b  on a.seq=b.seq order by state_dt asc) b 
 where a.id-b.id=-1) c
 ) d  group by cum_rise_day;


--stock_pool choose 
select distinct  stock_code from (select * from stock_all_plus a  where state_dt='2018-12-04' and 1>(select count(distinct stock_code) num from stock_all_plus b where state_dt='2018-12-04' and  b.amount>a.amount and b.industry=a.industry)) c
into outfile '/Users/wanghongbo8/fonts_whb/stock_list.csv' character set utf8 
 fields terminated by ',' optionally enclosed by '"' 
 lines terminated by '\n';


--创建参数表，写入本地分析待用
create table backtest_paraset(
        back_test_turn int(11)   AUTO_INCREMENT,
        start_dt varchar(50) NOT NULL DEFAULT '',
        end_dt  varchar(50) NOT NULL DEFAULT '',
        para_window int(11) DEFAULT NULL,
        strategy  varchar(50) NOT NULL DEFAULT 'mkwz',
        hold_days int(11) DEFAULT NULL,
        torlence_lost decimal(20,4) DEFAULT NULL,
        torlence_profit decimal(20,4) DEFAULT NULL,
        operate_days int(11) DEFAULT NULL,
        backtest_tm varchar(50) DEFAULT NULL,
        PRIMARY KEY (`back_test_turn`)
        )ENGINE=InnoDB AUTO_INCREMENT=181 DEFAULT CHARSET=gbk;