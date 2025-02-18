
----------------------------- HCM_SL_CSKHH_003

drop table temp_giao;
select* from temp_giao;


delete from dinhbien_cuahang where thang=202501;
insert into dinhbien_cuahang 
select 202501,ma_to,ma_pb,dbien_tong,dbien_cht,dbien_kiem,dbien_gdv,dmuc_haumai_ch,to_number(ldong_tte_gdv)
  from temp_giao;

select * from dinhbien_cuahang a where thang=202501;
select sum(dbien_tong),sum(dbien_cht),sum(dbien_kiem),sum(dbien_gdv),sum(ldong_tte_gdv),sum(dmuc_haumai_ch)
  from dinhbien_cuahang where thang=202501;


update bangluong_kpi a
   set giao=(select round(580*dbien_gdv/ldong_tte_gdv) from dinhbien_cuahang where thang=a.thang and ma_to=a.ma_to)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202501 and ma_kpi='HCM_SL_CSKHH_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202501 and upper(ma_kpi)='HCM_SL_CSKHH_003' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
 
 
update bangluong_kpi a
   set giao=(select dmuc_haumai_ch from dinhbien_cuahang where thang=a.thang and ma_to=a.ma_to) 
       --   update bangluong_kpi a set giao=''
       --   select sum(giao) from bangluong_kpi a
 where thang=202501 and ma_kpi='HCM_SL_CSKHH_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202501 and upper(ma_kpi)='HCM_SL_CSKHH_003' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);


 
 
----------------------------- HCM_SL_HOTRO_001

update bangluong_kpi a set giao=1050
       --update bangluong_kpi a set tytrong='',giao=''
       --select * from bangluong_kpi a
 where thang=202501 and ma_kpi='HCM_SL_HOTRO_001'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202501 and upper(ma_kpi)='HCM_SL_HOTRO_001' and to_truong_pho is null
                  and ma_vtcv=a.ma_vtcv);
 
 

----------------------------- HCM_DT_LUYKE_002

update bangluong_kpi a
   set giao=(select sum(tong_luyke) from ttkdhcm_ktnv.kehoach_606 where thang=a.thang and id_dv_606='0' and hrm=a.ma_nv)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202501 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202501 and upper(ma_kpi)='HCM_DT_LUYKE_002' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
                  
 
update bangluong_kpi a
   set giao=(select sum(tong_luyke) from ttkdhcm_ktnv.kehoach_606 where thang=a.thang and id_dv_606='0' and to_hrm=a.ma_to)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202501 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202501 and upper(ma_kpi)='HCM_DT_LUYKE_002' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);
                   
 
----- PGD
--select * from blkpi_dm_to_pgd where thang=202501 and ma_kpi='HCM_DT_LUYKE_002' and ma_pb in ('VNP0702300','VNP0702400','VNP0702500')

drop table temp_bsc_giao;
create table temp_bsc_giao as
select ma_nv,sum(tong_luyke)dthu from(
select (select distinct ma_nv from blkpi_dm_to_pgd 
         where thang=a.thang and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_to=a.to_hrm)ma_nv
      ,tong_luyke
  from ttkdhcm_ktnv.kehoach_606 a
 where thang=202501 and id_dv_606='0'
)group by ma_nv;
 
update bangluong_kpi a
   set giao=(select dthu from temp_bsc_giao where ma_nv=a.ma_nv)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202501 and ma_kpi='HCM_DT_LUYKE_002'    --and ma_nv='VNP017763'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202501 and upper(ma_kpi)='HCM_DT_LUYKE_002' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);



select ma_kpi,ma_vtcv,(select ten_vtcv from nhanvien where thang=a.thang and ma_vtcv=a.ma_vtcv and rownum=1)ten_vtcv
      ,(select count(*) from bangluong_kpi 
         where thang=a.thang and ma_kpi='HCM_DT_LUYKE_002' and giao>0 and ma_vtcv=a.ma_vtcv
         group by ma_vtcv)sl_nv
  from blkpi_danhmuc_kpi_vtcv a
 where thang=202501 and ma_kpi='HCM_DT_LUYKE_002'

----------------------------- HCM_DT_LUYKE_003
update bangluong_kpi a
   set giao=(select sum(tong_luyke) from ttkdhcm_ktnv.kehoach_606 where thang=a.thang and id_dv_606='1.1' and hrm=a.ma_nv)
      --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202501 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202501 and upper(ma_kpi)='HCM_DT_LUYKE_003' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
                  
 
update bangluong_kpi a
   set giao=(select sum(tong_luyke) from ttkdhcm_ktnv.kehoach_606 where thang=a.thang and id_dv_606='1.1' and to_hrm=a.ma_to)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202501 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202501 and upper(ma_kpi)='HCM_DT_LUYKE_003' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);
                      
                  
----- PGD
drop table temp_bsc_giao;
create table temp_bsc_giao as
select ma_nv,sum(tong_luyke)dthu from(
select (select distinct ma_nv from blkpi_dm_to_pgd 
         where thang=a.thang and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_to=a.to_hrm)ma_nv
       ,tong_luyke
  from ttkdhcm_ktnv.kehoach_606 a
 where thang=202501 and id_dv_606='1.1'
)group by ma_nv;

update bangluong_kpi a
   set giao=(select dthu from temp_bsc_giao where ma_nv=a.ma_nv)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202501 and ma_kpi='HCM_DT_LUYKE_003' 
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202501 and upper(ma_kpi)='HCM_DT_LUYKE_003' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);
                     
 
---- Kiem tra
select ma_kpi,ma_vtcv,(select ten_vtcv from nhanvien where thang=a.thang and ma_vtcv=a.ma_vtcv and rownum=1)ten_vtcv
      ,(select count(*) from bangluong_kpi 
         where thang=a.thang and ma_kpi='HCM_DT_LUYKE_003' and giao>0 and ma_vtcv=a.ma_vtcv
         group by ma_vtcv)sl_nv
  from blkpi_danhmuc_kpi_vtcv a
 where thang=202501 and ma_kpi='HCM_DT_LUYKE_003'

 
 

----------------HCM_DT_PTNAM_006
update bangluong_kpi a
   set giao=(select sum(ptm_luyke) from ttkdhcm_ktnv.kehoach_606 where thang=a.thang and id_dv_606='0' and hrm=a.ma_nv)
       --   update bangluong_kpi a set giao='',thuchien=''
       --   select * from bangluong_kpi a
 where thang=202501 and ma_kpi='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202501 and upper(ma_kpi)='HCM_DT_PTNAM_006' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
                  
 
update bangluong_kpi a
   set giao=(select sum(ptm_luyke) from ttkdhcm_ktnv.kehoach_606 where thang=a.thang and id_dv_606='0' and to_hrm=a.ma_to)
       --   update bangluong_kpi a set giao='',thuchien=''
       --   select * from bangluong_kpi a
 where thang=202501 and ma_kpi='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202501 and upper(ma_kpi)='HCM_DT_PTNAM_006' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);
                  
                   
----- PGD
drop table temp_bsc_giao;
create table temp_bsc_giao as
select ma_nv,sum(ptm_luyke)dthu 
  from(select (select distinct ma_nv from blkpi_dm_to_pgd 
                where thang=a.thang and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_to=a.to_hrm)ma_nv
             ,ptm_luyke
         from ttkdhcm_ktnv.kehoach_606 a
        where thang=202501 and id_dv_606='0'
)group by ma_nv;
 
update bangluong_kpi a
   set giao=(select dthu from temp_bsc_giao where ma_nv=a.ma_nv)
       --   update bangluong_kpi a set giao=''
       --   select * from bangluong_kpi a
 where thang=202501 and upper(ma_kpi)='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=a.thang and upper(ma_kpi)=upper(a.ma_kpi) and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);
                  
             
 

select ma_kpi,ma_vtcv,(select ten_vtcv from nhanvien where thang=a.thang and ma_vtcv=a.ma_vtcv and rownum=1)ten_vtcv
      ,(select count(*) from bangluong_kpi
         where thang=a.thang and giao>0 and ma_vtcv=a.ma_vtcv and ma_kpi=a.ma_kpi
         group by ma_vtcv)sl_nv
  from blkpi_danhmuc_kpi_vtcv a
 where thang=202501 and ma_kpi='HCM_DT_PTNAM_006' 
 
 
 
 
--------------------- HCM_SL_DAILY_003
 
update bangluong_kpi a set giao=1
       --select * from bangluong_kpi a
       --update bangluong_kpi a set giao=''
 where thang=202501 and ma_kpi='HCM_SL_DAILY_003'
   and exists(select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
               where thang=202501 and ma_kpi='HCM_SL_DAILY_003'
                 and to_truong_pho is null and giamdoc_phogiamdoc is null
                 and ma_vtcv=a.ma_vtcv);
  


 
 
 
 
 