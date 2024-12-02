
----------------------------- HCM_SL_CSKHH_003

drop table temp_giao;
select* from temp_giao;


delete from dinhbien_cuahang where thang=202411;
insert into dinhbien_cuahang 
select 202411,ma_to,ma_pb,dbien_tong,dbien_cht,dbien_kiem,dbien_gdv,dmuc_haumai_ch,ldong_tte_gdv 
  from temp_giao;

select * from dinhbien_cuahang where thang=202411;
select sum(dbien_tong),sum(dbien_cht),sum(dbien_kiem),sum(dbien_gdv),sum(ldong_tte_gdv),sum(dmuc_haumai_ch)
  from dinhbien_cuahang where thang=202411;


update bangluong_kpi a
   set giao=(select round(580*dbien_gdv/ldong_tte_gdv) from dinhbien_cuahang where thang=a.thang and ma_to=a.ma_to)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_SL_CSKHH_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_SL_CSKHH_003' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
 
 
update bangluong_kpi a
   set giao=(select dmuc_haumai_ch from dinhbien_cuahang where thang=a.thang and ma_to=a.ma_to) 
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_SL_CSKHH_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_SL_CSKHH_003' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);

/*
update bangluong_kpi a
   set giao=(select dmuc_haumai_ch from dinhbien_cuahang where thang=a.thang and ma_to=a.ma_to) 
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_SL_CSKHH_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_SL_CSKHH_003' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);
 */                 


update bangluong_kpi set tytrong=20      
       --   update bangluong_kpi a set tytrong=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_SL_CSKHH_003';
 
 
 
 
----------------------------- HCM_SL_HOTRO_001

update bangluong_kpi a
   set tytrong=100 ,giao=1050
       --update bangluong_kpi a set tytrong='',giao=''
       --select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_SL_HOTRO_001'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_SL_HOTRO_001' and to_truong_pho is null
                  and ma_vtcv=a.ma_vtcv);
 
 

----------------------------- HCM_DT_LUYKE_002

update bangluong_kpi a
   set giao=(select sum(tong_luyke) from ttkdhcm_ktnv.kehoach_606 where thang=a.thang and id_dv_606='0' and hrm=a.ma_nv)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_DT_LUYKE_002' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
                  
 
update bangluong_kpi a
   set giao=(select sum(tong_luyke) from ttkdhcm_ktnv.kehoach_606 where thang=a.thang and id_dv_606='0' and to_hrm=a.ma_to)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_DT_LUYKE_002' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);
 
update bangluong_kpi a
   set giao=giao+
            case when ma_to='VNP0702406' --line SBN
                          then (select sum(tong_luyke) from ttkdhcm_ktnv.kehoach_606
                                 where thang=202407 and id_dv_606='0' and to_hrm='VNP0702413') --line dv so 1
                     when ma_to='VNP0702416' --SME4
                          then (select sum(tong_luyke) from ttkdhcm_ktnv.kehoach_606
                                 where thang=202407 and id_dv_606='0' and to_hrm='VNP0702414') --line dv so 2    
                 end  
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_DT_LUYKE_002' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv)
   and ma_to in ('VNP0702406','VNP0702416'); -- SBH, SME4
                  
 
----- PGD
drop table temp_bsc_giao;
create table temp_bsc_giao as
select ma_nv,sum(tong_luyke)dthu from(
select (select distinct ma_nv from blkpi_dm_to_pgd 
         where thang=a.thang and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_to=a.to_hrm)ma_nv
      ,tong_luyke
  from ttkdhcm_ktnv.kehoach_606 a
 where thang=202411 and id_dv_606='0'
)group by ma_nv;
 
update bangluong_kpi a
   set giao=(select dthu from temp_bsc_giao where ma_nv=a.ma_nv)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_DT_LUYKE_002' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);



----------------------------- HCM_DT_LUYKE_003

update bangluong_kpi a
   set giao=(select sum(tong_luyke) from ttkdhcm_ktnv.kehoach_606 where thang=a.thang and id_dv_606='1.1' and hrm=a.ma_nv)
      --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_DT_LUYKE_003' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
                  
 
update bangluong_kpi a
   set giao=(select sum(tong_luyke) from ttkdhcm_ktnv.kehoach_606 where thang=a.thang and id_dv_606='1.1' and to_hrm=a.ma_to)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_DT_LUYKE_003' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);
                       
update bangluong_kpi a
   set giao=giao+
            case when ma_to='VNP0702406' --line SBN
                          then (select sum(tong_luyke) from ttkdhcm_ktnv.kehoach_606
                                 where thang=202407 and id_dv_606='1.1' and to_hrm='VNP0702413') --line dv so 1
                     when ma_to='VNP0702416' --SME4
                          then (select sum(tong_luyke) from ttkdhcm_ktnv.kehoach_606
                                 where thang=202407 and id_dv_606='1.1' and to_hrm='VNP0702414') --line dv so 2    
                 end 
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_DT_LUYKE_003' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv)
   and ma_to in ('VNP0702406','VNP0702416');
                   
 
----- PGD
drop table temp_bsc_giao;
create table temp_bsc_giao as
select ma_nv,sum(tong_luyke)dthu from(
select (select distinct ma_nv from blkpi_dm_to_pgd 
         where thang=a.thang and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_to=a.to_hrm)ma_nv
       ,tong_luyke
  from ttkdhcm_ktnv.kehoach_606 a
 where thang=202411 and id_dv_606='1.1'
)group by ma_nv;

update bangluong_kpi a
   set giao=(select dthu from temp_bsc_giao where ma_nv=a.ma_nv)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_DT_LUYKE_003' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);
                     
 
---- Kiem tra
select ma_kpi,ma_vtcv,(select ten_vtcv from nhanvien where thang=a.thang and ma_vtcv=a.ma_vtcv and rownum=1)ten_vtcv
      ,(select count(*) from bangluong_kpi 
         where thang=a.thang and ma_kpi='HCM_DT_LUYKE_003' and giao>0 and ma_vtcv=a.ma_vtcv
         group by ma_vtcv)sl_nv
  from blkpi_danhmuc_kpi_vtcv a
 where thang=202411 and ma_kpi='HCM_DT_LUYKE_003'

 
 

----------------HCM_DT_PTNAM_006

update bangluong_kpi a
   set giao=(select sum(ptm_luyke) from ttkdhcm_ktnv.kehoach_606 where thang=a.thang and id_dv_606='0' and hrm=a.ma_nv)
       --   update bangluong_kpi a set giao='',thuchien=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_DT_PTNAM_006' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
                  
 
----- TT
update bangluong_kpi a
   set giao=(select sum(ptm_luyke) from ttkdhcm_ktnv.kehoach_606 where thang=a.thang and id_dv_606='0' and to_hrm=a.ma_to)
       --   update bangluong_kpi a set giao='',thuchien=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_DT_PTNAM_006' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);
                                  
update bangluong_kpi a
   set giao=giao+
            case when ma_to='VNP0702406' --line SBN
                          then (select sum(ptm_luyke) from ttkdhcm_ktnv.kehoach_606
                                 where thang=202407 and id_dv_606='0' and to_hrm='VNP0702413') --line dv so 1
                     when ma_to='VNP0702416' --SME4
                          then (select sum(ptm_luyke) from ttkdhcm_ktnv.kehoach_606
                                 where thang=202407 and id_dv_606='0' and to_hrm='VNP0702414') --line dv so 2    
                 end
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_DT_PTNAM_006' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv)
   and ma_to in ('VNP0702406','VNP0702416');
                  
                   
----- PGD
drop table temp_bsc_giao;
create table temp_bsc_giao as
select ma_nv,sum(ptm_luyke)dthu 
  from(select (select distinct ma_nv from blkpi_dm_to_pgd 
                where thang=a.thang and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_to=a.to_hrm)ma_nv
             ,ptm_luyke
         from ttkdhcm_ktnv.kehoach_606 a
        where thang=202411 and id_dv_606='0'
)group by ma_nv;
 
update bangluong_kpi a
   set giao=(select dthu from temp_bsc_giao where ma_nv=a.ma_nv)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202411 and upper(ma_kpi)='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=a.thang and upper(ma_kpi)=upper(a.ma_kpi) and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);
                  
             
 

select ma_kpi,ma_vtcv,(select ten_vtcv from nhanvien where thang=a.thang and ma_vtcv=a.ma_vtcv and rownum=1)ten_vtcv
      ,(select count(*) from bangluong_kpi
         where thang=a.thang and giao is not null and ma_vtcv=a.ma_vtcv and ma_kpi=a.ma_kpi
         group by ma_vtcv)sl_nv
  from blkpi_danhmuc_kpi_vtcv a
 where thang=202411 and ma_kpi='HCM_DT_PTNAM_006' 
 
 
 
 
  
-------------------------HCM_DT_PTMOI_054

select distinct ten_kpi from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG where thang=202411;

select a.*,(select ten_vtcv from nhanvien where thang=202411 and ma_nv=a.ma_nv)  
  from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG a 
 where thang=202411 and ten_kpi like '4.%';


select * from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG where thang=202411 and ten_kpi like '4.%' and ma_nv in(
select ma_nv from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG where thang=202411 and ten_kpi like '4.%' group by ma_nv having count(*)>1)


update bangluong_kpi a
   set giao=(select sum(sogiao) from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG where thang=202411 and ten_kpi like '4.%' and ma_nv=a.ma_nv)
       --update bangluong_kpi a set giao=''
       --select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_PTMOI_054';
 
 
-- Kiem tra
select x.ma_nv manv_372,sogiao,y.ma_nv manv_kpi,giao from
(select ma_nv,sogiao
  from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG a
 where thang=202411 and ten_kpi like '4.%' and ma_nv is not null
 and ma_pb in ('VNP0702300','VNP0702400','VNP0702500')
 )x
 full join 
 (select ma_nv,giao from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_PTMOI_054' and giao is not null)y
 on x.ma_nv=y.ma_nv
 where sogiao<>giao
 

select 'id372'nd,count(*)sl,sum(sogiao)giao from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG a
 where thang=202411 and ten_kpi like '4.%' and ma_nv is not null 
   and ma_pb in ('VNP0702300','VNP0702400','VNP0702500')
 union 
select 'bangluong',count(*)sl,sum(giao)giao from bangluong_kpi where thang=202411 and ma_kpi='HCM_DT_PTMOI_054' and giao is not null;
 

select x.ma_vtcv ma_vtcv_id372,y.ma_vtcv ma_vtcv_dm_vtcv 
      ,(select ten_vtcv from nhanvien where thang=202411 and ma_vtcv=nvl(x.ma_vtcv,y.ma_vtcv) and rownum=1)ten_vtcv
  from (select distinct (select ma_vtcv from nhanvien where thang=202411 and ma_nv=a.ma_nv)ma_vtcv  
          from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG a 
         where thang=202411 and ten_kpi like '4.%')x
       full join 
       (select * from blkpi_danhmuc_kpi_vtcv where thang=202411 and ma_kpi='HCM_DT_PTMOI_054')y
       on x.ma_vtcv=y.ma_vtcv
 ;



drop table temp_bsc;
create table temp_bsc as 
select ma_nv,sum(sogiao)sogiao,(select ma_pb from nhanvien where thang=202411 and ma_nv=b.ma_nv)ma_pb
  from( 
select sogiao
      ,(select ma_nv from blkpi_dm_to_pgd 
         where thang=202411 and ma_pb in ('VNP0702300','VNP0702400','VNP0702500')
           and ma_to=a.ma_to)ma_nv
  from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG a
 where thang=202411 and ten_kpi like '4.%' and ten_to like 'NV-%' and ma_pb in ('VNP0702300','VNP0702400','VNP0702500')
)b group by ma_nv;

update bangluong_kpi a
   set giao=(select sogiao from temp_bsc where ma_nv=a.ma_nv) 
       --update bangluong_kpi a set giao=''
       --select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_PTMOI_054'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_DT_PTMOI_054' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);

--Kiem tra -> sogiao id372>bangluong do co GD phu trach line  
select 'id372'nd,ma_pb,sogiao 
  from ttkdhcm_ktnv.ID372_GIAO_C2_CHOTTHANG 
 where thang=202411 and ten_kpi like '4.%' and ma_pb is not null and ma_nv is null
 union
select 'bangluong',ma_pb,sum(giao) from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_PTMOI_054'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202411 and upper(ma_kpi)='HCM_DT_PTMOI_054' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv)
 group by ma_pb
 order by ma_pb,nd;
                  
 

---------- Ty trong
update bangluong_kpi a
   set tytrong=case when exists(select * from blkpi_danhmuc_kpi_vtcv 
                                 where thang=202411 and upper(ma_kpi)='HCM_DT_PTMOI_054' 
                                   and to_truong_pho is null and giamdoc_phogiamdoc is null
                                   and ma_vtcv=a.ma_vtcv)
                        then 10
                    when ma_nv='VNP017445' then 10 -- TL QLDL DN1
                    when exists(select * from blkpi_danhmuc_kpi_vtcv 
                                 where thang=202411 and upper(ma_kpi)='HCM_DT_PTMOI_054' 
                                   and to_truong_pho is not null
                                   and ma_vtcv=a.ma_vtcv)
                        then 15
                    when ma_nv in ('VNP017621','VNP017699') -- Nhien, Yen
                        then 10
                    when exists(select * from blkpi_danhmuc_kpi_vtcv 
                                 where thang=202411 and upper(ma_kpi)='HCM_DT_PTMOI_054' 
                                   and giamdoc_phogiamdoc is not null
                                   and ma_vtcv=a.ma_vtcv)
                        then 15
                end
       --update bangluong_kpi a tytrong giao=''
       --select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_PTMOI_054'; 
 
 
 
 
 
--------------------- HCM_SL_DAILY_003
 
update bangluong_kpi a set giao=1
       --select * from bangluong_kpi a
       --update bangluong_kpi a set giao=''
 where thang=202411 and ma_kpi='HCM_SL_DAILY_003'
   and exists(select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
               where thang=202411 and ma_kpi='HCM_SL_DAILY_003'
                 and to_truong_pho is null and giamdoc_phogiamdoc is null
                 and ma_vtcv=a.ma_vtcv);
  

update bangluong_kpi a 
   set giao=(select count(*) from nhanvien where ma_vtcv='VNP-HNHCM_KHDN_3.1' and thang=a.thang and ma_to=a.ma_to)
       --select * from bangluong_kpi a
       --update bangluong_kpi a set giao=''
 where thang=202411 and ma_kpi='HCM_SL_DAILY_003'
   and exists(select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
               where thang=202411 and ma_kpi='HCM_SL_DAILY_003'
                 and to_truong_pho is not null
                 and ma_vtcv=a.ma_vtcv);


update bangluong_kpi set giao='' where thang=202411 and ma_kpi='HCM_SL_DAILY_003' and giao=0;
 
 
 
 
 
 
 