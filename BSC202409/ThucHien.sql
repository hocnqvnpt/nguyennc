
----------------------------------------HCM_SL_DAILY_003

insert into ptm_daily_cops_dthu
select thang_ptm,ma_tb,ma_gd,ten_tb,dthu_goi,ma_nguoigt,cast(null as varchar(10))duoctinh,ma_pb,ma_to,manv_ptm ma_nv,thang_ptm thang
         --,trangthaitb_id,chuquan_id
 from ttkd_bsc.ct_bsc_ptm a
 where thang_ptm=202410
   and (ma_nguoigt in (select ma_daily from ttkd_bsc.dm_daily_khdn where thang=a.thang_ptm and loai_hopdong='DAI LY MOI'))
   and trangthaitb_id=1 and chuquan_id=145
   and exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv where thang=202410 and ma_kpi='HCM_SL_DAILY_003' and ma_vtcv=a.ma_vtcv);

update ptm_daily_cops_dthu a set duoctinh='duoc tinh'
 where thang_ptm=202410
   and exists(select ma_nguoigt  
                from ptm_daily_cops_dthu
               where thang_ptm=a.thang_ptm and ma_nguoigt=a.ma_nguoigt
               group by ma_nguoigt
              having sum(dthu_goi)>=3000000);
commit;



update bangluong_kpi a
   set thuchien=(select count(distinct ma_nguoigt) from ptm_daily_cops_dthu where duoctinh='duoc tinh' and ma_nv=a.ma_nv and thang_ptm=a.thang)
       --select * from bangluong_kpi a
       --update bangluong_kpi a set thuchien=''
 where thang=202410 and ma_kpi='HCM_SL_DAILY_003'
   and exists(select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
               where thang=202410 and ma_kpi='HCM_SL_DAILY_003'
                 and to_truong_pho is null and giamdoc_phogiamdoc is null
                 and ma_vtcv=a.ma_vtcv);
  

update bangluong_kpi a
   set thuchien=(select count(distinct ma_nguoigt) from ptm_daily_cops_dthu where duoctinh='duoc tinh' and ma_to=a.ma_to and thang_ptm=a.thang)
       --select * from bangluong_kpi a
       --update bangluong_kpi a set thuchien=''
 where thang=202410 and ma_kpi='HCM_SL_DAILY_003'
   and exists(select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
               where thang=202410 and ma_kpi='HCM_SL_DAILY_003'
                 and to_truong_pho is not null
                 and ma_vtcv=a.ma_vtcv);
 
   

update bangluong_kpi a
   set tyle_thuchien=round(thuchien/giao*100,2)
      ,mucdo_hoanthanh=least(round(thuchien/giao*100,2),120)
       --select * from bangluong_kpi a
       --update bangluong_kpi a set thuchien=''
 where thang=202410 and ma_kpi='HCM_SL_DAILY_003' and giao>0;
 

----------------------------------------HCM_DT_PTMOI_056

insert into ct_bsc_ptmoi_056
       (thang,ma_pb,ma_to,ma_nv,ma_vtcv
       ,cntt_dthu_giao,cntt_dthu_thuchien,cdbr_mytv_dthu_giao,cdbr_mytv_dthu_thuchien,didong_dthu_giao,didong_dthu_thuchien)   
select 202410,ma_pb,ma_to,ma_nv,ma_vtcv
      ,nhomcntt_dtgiao,nhomcntt_kqth,nhombrcd_dtgiao,nhombrcd_kqth
      ,nhomvina_dtgiao,nvl(nhomvinats_kqth,0)+nvl(nhomvinatt_kqth,0)nhomvina_kqth
  from dinhmuc_giao_dthu_ptm a
 where thang=202410-- and tinh_bsc=1
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202410 and ma_kpi='HCM_DT_PTMOI_056' and to_truong_pho is null and giamdoc_phogiamdoc is null 
                 and ma_vtcv=a.ma_vtcv)
;
   
update ct_bsc_ptmoi_056 a
   set cntt_tyle_thuchien=case when cntt_dthu_giao>0 then round(nvl(cntt_dthu_thuchien,0)/cntt_dthu_giao*100,2) end
      ,cntt_diem_tru_bsc=case when cntt_dthu_giao>0 and nvl(cntt_dthu_thuchien,0)<cntt_dthu_giao*0.9 then 5 end
      ,cdbr_mytv_tyle_thuchien=case when cdbr_mytv_dthu_giao>0 then round(nvl(cdbr_mytv_dthu_thuchien,0)/cdbr_mytv_dthu_giao*100,2) end
      ,cdbr_mytv_diem_tru_bsc=case when cdbr_mytv_dthu_giao>0 and nvl(cdbr_mytv_dthu_thuchien,0)<cdbr_mytv_dthu_giao*0.9 then 5 end
      ,didong_tyle_thuchien=case when didong_dthu_giao>0 then round(nvl(didong_dthu_thuchien,0)/didong_dthu_giao*100,2) end
      ,didong_diem_tru_bsc=case when didong_dthu_giao>0 and nvl(didong_dthu_thuchien,0)<didong_dthu_giao*0.9 then 5 end   
 where thang=202410;

update ct_bsc_ptmoi_056 a
   set tong_diem_tru_bsc=nvl(cntt_diem_tru_bsc,0)+nvl(cdbr_mytv_diem_tru_bsc,0)+nvl(didong_diem_tru_bsc,0)
 where thang=202410;   
update ct_bsc_ptmoi_056 set tong_diem_tru_bsc='' where thang=202410 and tong_diem_tru_bsc=0; 
 


update bangluong_kpi a
   set diem_tru=(select tong_diem_tru_bsc from ct_bsc_ptmoi_056 where thang=a.thang and ma_nv=a.ma_nv)
       --update bangluong_kpi a set diem_tru=''
       --select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_PTMOI_056'
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202410 and ma_kpi='HCM_DT_PTMOI_056' and to_truong_pho is null and giamdoc_phogiamdoc is null
                 and ma_vtcv=a.ma_vtcv);
                 
                 
----------------------------------------HCM_DT_PTMOI_054
insert into ct_dvs_pkhdn_2024
select thang_ptm,ma_gd,ma_tb,ma_kh,ten_tb,dich_vu
      ,(select pdh_nhom_dvu_khdn_2024 from dm_loaihinh_hsqd where loaitb_id=a.loaitb_id)nhom_dichvu
      ,dthu_goi
      ,(select trangthai_tb from css_hcm.trangthai_tb where trangthaitb_id=a.trangthaitb_id)trangthai_tb
      ,(select tenchuquan from css_hcm.chuquan where chuquan_id=a.chuquan_id)chuquan,lydo_khongtinh_luong
      ,case when (trangthaitb_id=1 or trangthaitb_id is null or (loaitb_id in (20,149) and trangthaitb_id=10))
                 and chuquan_id in (145,264)
                then 1 end duoc_tinh
      ,ma_pb,ma_to,manv_ptm ma_nv,id
  from ttkd_bsc.ct_bsc_ptm a
 where thang_ptm=202410 and dthu_goi<>0
   and exists(select * from dm_loaihinh_hsqd where pdh_nhom_dvu_khdn_2024 is not null and loaitb_id=a.loaitb_id)
   --and dthu_goi<>(select dthu_goi from ttkd_bsc.ct_bsc_ptm where id=a.id)--kiem tra
;



update bangluong_kpi a
   set thuchien=(select round(sum(dthu_goi)/1000000,3) from ct_dvs_pkhdn_2024 where duoc_tinh=1 and ma_nv=a.ma_nv) 
       --update bangluong_kpi a set thuchien=''
       --select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_PTMOI_054'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_PTMOI_054' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);

                                   
update bangluong_kpi a
   set thuchien=(select round(sum(dthu_goi)/1000000,3) from ct_dvs_pkhdn_2024 where duoc_tinh=1 and ma_to=a.ma_to) 
      --update bangluong_kpi a set thuchien=''
      --    select * from bangluong_kpi a                         
 where thang=202410 and ma_kpi='HCM_DT_PTMOI_054'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_PTMOI_054' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);


update bangluong_kpi a
   set thuchien=thuchien+
                case when ma_to='VNP0702406' --line SBN
                          then (select round(sum(dthu_goi)/1000000,3) from ct_dvs_pkhdn_2024 
                                 where duoc_tinh=1 and thang_ptm<=202407 and ma_to='VNP0702413') --line dv so 1
                     when ma_to='VNP0702416' --SME4
                          then (select round(sum(dthu_goi)/1000000,3) from ct_dvs_pkhdn_2024 
                                 where duoc_tinh=1 and thang_ptm<=202407 and ma_to='VNP0702414') --line dv so 2    
                 end         
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_PTMOI_054'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_PTMOI_054' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv)
   and ma_to in ('VNP0702406','VNP0702416'); -- SBH, SME4 
 
                  
drop table temp_bsc;
create table temp_bsc as
select dthu_goi
      ,(select ma_nv from blkpi_dm_to_pgd 
         where thang=202410 and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_to=a.ma_to)ma_nv
  from ct_dvs_pkhdn_2024 a
 where duoc_tinh=1
 
 union all
select dthu_goi
      ,(select ma_nv from blkpi_dm_to_pgd 
         where thang=202410 and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') 
           and ma_to=decode(a.ma_to,'VNP0702413','VNP0702406','VNP0702416'))ma_nv
  from ct_dvs_pkhdn_2024 a
 where duoc_tinh=1 and thang_ptm<=202407 and ma_to in ('VNP0702413','VNP0702414')
;

update bangluong_kpi a
   set thuchien=(select round(sum(dthu_goi)/1000000,3) from temp_bsc where ma_nv=a.ma_nv) 
      -- update bangluong_kpi a set thuchien=''
      --    select * from bangluong_kpi a                            
 where thang=202410 and ma_kpi='HCM_DT_PTMOI_054'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_PTMOI_054' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);


update bangluong_kpi 
   set tyle_thuchien=least(round(thuchien/giao*100,2),120)
      ,mucdo_hoanthanh=least(round(thuchien/giao*100,2),120)
      -- update bangluong_kpi set tyle_thuchien=''
      --    select * from bangluong_kpi                            
 where thang=202410 and ma_kpi='HCM_DT_PTMOI_054';
 
 
 
----------------------------------------HCM_DT_LUYKE_002
  
update bangluong_kpi a
   set thuchien=(select round(sum(th_luyke_tong)/1000000,3) from nguyennc.ktkh_bcao_9a_th 
                  where thang=a.thang and stt='0' and ma_nv=a.ma_nv group by ma_nv)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_LUYKE_002' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
                  

update bangluong_kpi a
   set thuchien=(select round(sum(th_luyke_tong)/1000000,3) from nguyennc.ktkh_bcao_9a_th 
                  where thang=a.thang and stt='0' and ma_to=a.ma_to)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_LUYKE_002' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);
 

update bangluong_kpi a
   set thuchien=thuchien+
                case when ma_to='VNP0702406'
                          then (select round(sum(th_luyke_tong)/1000000,3) from nguyennc.ktkh_bcao_9a_th 
                                 where thang=202407 and stt='0' and ma_to='VNP0702413') --line dv so 1
                     when ma_to='VNP0702416' --SME4
                          then (select round(sum(th_luyke_tong)/1000000,3) from nguyennc.ktkh_bcao_9a_th
                                 where thang=202407 and stt='0' and ma_to='VNP0702414') --line dv so 2    
                 end         
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_LUYKE_002' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv)
   and ma_to in ('VNP0702406','VNP0702416');
                  
 

drop table temp_bsc_thuchien;
create table temp_bsc_thuchien as
select ma_nv,round(sum(th_luyke_tong)/1000000,3)dthu from (
select (select distinct ma_nv from blkpi_dm_to_pgd 
         where thang=a.thang and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_to=a.ma_to)ma_nv
      ,th_luyke_tong
  from nguyennc.ktkh_bcao_9a_th a
 where thang=202410 and stt='0'
)group by ma_nv;

 
update bangluong_kpi a
   set thuchien=(select dthu from temp_bsc_thuchien where ma_nv=a.ma_nv)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_LUYKE_002' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);
                  
          
                              

update bangluong_kpi a set tyle_thuchien=round(thuchien/giao*100,2)
       --   update bangluong_kpi set tyle_thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_LUYKE_002';


update bangluong_kpi a 
   set mucdo_hoanthanh=least(case when tyle_thuchien<80 then 0
                                  when tyle_thuchien>=80 and tyle_thuchien<87 then 80-4*(87-tyle_thuchien)
                                  when tyle_thuchien>=87 and tyle_thuchien<92 then 87-2*(92-tyle_thuchien)
                                  when tyle_thuchien>=92 and tyle_thuchien<97 then 97-2*(97-tyle_thuchien)
                                  when tyle_thuchien>=97 and tyle_thuchien<100 then 100+2*(tyle_thuchien-97)
                                  when tyle_thuchien>=100 then 100+3*(tyle_thuchien-97)
                              end,150)     
-- MÐHT t?i da 150%.
       --   update bangluong_kpi set tyle=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_LUYKE_002';





-------HCM_DT_LUYKE_003
  
update bangluong_kpi a
   set thuchien=(select round(sum(th_luyke_tong)/1000000,3) from nguyennc.ktkh_bcao_9a_th where thang=a.thang and stt='1.1' and ma_nv=a.ma_nv)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_LUYKE_003' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
                  
 
update bangluong_kpi a
   set thuchien=(select round(sum(th_luyke_tong)/1000000,3) from nguyennc.ktkh_bcao_9a_th where thang=a.thang and stt='1.1' and ma_to=a.ma_to)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_LUYKE_003' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);
                       
 
update bangluong_kpi a
   set thuchien=thuchien+
                case when ma_to='VNP0702406' --line SBN
                          then (select round(sum(th_luyke_tong)/1000000,3) from nguyennc.ktkh_bcao_9a_th 
                                 where thang=202407 and stt='1.1' and ma_to='VNP0702413') --line dv so 1
                     when ma_to='VNP0702416' --SME4
                          then (select round(sum(th_luyke_tong)/1000000,3) from nguyennc.ktkh_bcao_9a_th 
                                 where thang=202407 and stt='1.1' and ma_to='VNP0702414') --line dv so 2 
                 end               
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_LUYKE_003' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv)
   and ma_to in ('VNP0702406','VNP0702416');
                   
 
drop table temp_bsc_thuchien;
create table temp_bsc_thuchien as
select ma_nv,round(sum(th_luyke_tong)/1000000,3)dthu from (
select (select distinct ma_nv from blkpi_dm_to_pgd 
         where thang=a.thang and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_to=a.ma_to)ma_nv
      ,th_luyke_tong
  from nguyennc.ktkh_bcao_9a_th a
 where thang=202410 and stt='1.1'
)group by ma_nv;


update bangluong_kpi a
   set thuchien=(select dthu from temp_bsc_thuchien where ma_nv=a.ma_nv)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_LUYKE_003' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);
                     
 
   
update bangluong_kpi a set tyle_thuchien=round(thuchien/giao*100,2)
       --   update bangluong_kpi set tyle=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_LUYKE_003' and giao>0;
 
 
 
update bangluong_kpi a 
   set mucdo_hoanthanh=least(case when tyle_thuchien<30 then 0 else 1.1*tyle_thuchien end,150)     
       --   update bangluong_kpi set tyle=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_LUYKE_003';


 
 
----------------HCM_DT_PTNAM_006

update bangluong_kpi a
   set thuchien=(select round(sum(th_luyke_ptm)/1000000,3) from nguyennc.ktkh_bcao_9a_th where thang=a.thang and stt='0' and ma_nv=a.ma_nv)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_PTNAM_006' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
                  
 
update bangluong_kpi a
   set thuchien=(select round(sum(th_luyke_ptm)/1000000,3) from nguyennc.ktkh_bcao_9a_th where thang=a.thang and stt='0' and ma_to=a.ma_to)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_PTNAM_006' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);
                                  
 
update bangluong_kpi a
   set thuchien=thuchien+
                case when ma_to='VNP0702406' --line SBN
                          then (select round(sum(th_luyke_ptm)/1000000,3) from nguyennc.ktkh_bcao_9a_th 
                                 where thang=202407 and stt='0' and ma_to='VNP0702413') --line dv so 1
                     when ma_to='VNP0702416' --SME4
                          then (select round(sum(th_luyke_ptm)/1000000,3) from nguyennc.ktkh_bcao_9a_th 
                                 where thang=202407 and stt='0' and ma_to='VNP0702414') --line dv so 2  
                 end            
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_DT_PTNAM_006' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv)
   and ma_to in ('VNP0702406','VNP0702416');
                  
                   

drop table temp_bsc_thuchien;
create table temp_bsc_thuchien as
select ma_nv,round(sum(th_luyke_ptm)/1000000,3)dthu 
  from(select (select distinct ma_nv from blkpi_dm_to_pgd 
                where thang=a.thang and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_to=a.ma_to)ma_nv
             ,th_luyke_ptm
         from nguyennc.ktkh_bcao_9a_th a
        where thang=202410 and stt='0'
)group by ma_nv;

 
update bangluong_kpi a
   set thuchien=(select dthu from temp_bsc_thuchien where ma_nv=a.ma_nv)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and upper(ma_kpi)='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=a.thang and upper(ma_kpi)=upper(a.ma_kpi) and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);
                  
             
      

update bangluong_kpi a set tyle_thuchien=round(thuchien/giao*100,2)
       --   update bangluong_kpi set tyle=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_PTNAM_006' and giao>0;
 
 
update bangluong_kpi a 
   set mucdo_hoanthanh=round(least(case when tyle_thuchien<30 then 0 else 1.25*tyle_thuchien end,150),2)     
       --   update bangluong_kpi set tyle=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_PTNAM_006';
  
 
 

----------------------------------------HCM_CL_DHQLY_007

delete from ct_bsc_cl_dhqly_007_bhol where thang=202410;
insert into ct_bsc_cl_dhqly_007_bhol
select distinct a.thang,a.ma_pb,a.ten_pb,a.ma_to,a.ten_to,b.ma_nv,a.ma_nv ma_nhanvien,a.ten_nv ten_nhanvien,a.ma_vtcv,a.ten_vtcv
      ,(select mucdo_hoanthanh from bangluong_kpi where thang=a.thang and upper(ma_kpi)='HCM_DT_PTMOI_021' and ma_nv=a.ma_nv)mdo_hthanh_dthu_ptm
      ,cast(null as number)sl_dat,cast(null as number)sl_khongdat,cast(null as number)tyle_thuchien
      ,cast(null as number)diem_cong,cast(null as number)diem_tru
  from nhanvien a,nhanvien b
 where a.thang=202410 and a.tinh_bsc=1 and a.thaydoi_vtcv=0 and a.thang=b.thang and a.ma_to=b.ma_to
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202410 and ma_kpi='HCM_CL_DHQLY_007' and ma_vtcv='VNP-HNHCM_BHKV_52'
                 and ma_vtcv=b.ma_vtcv)
 order by a.ma_to,a.ma_vtcv desc,a.ma_nv;

update ct_bsc_cl_dhqly_007_bhol a
   set sl_dat=case when mdo_hthanh_dthu_ptm>=100 then 1 end
      ,sl_khongdat=case when mdo_hthanh_dthu_ptm<100 then 1 end
 where thang=202410 
   and not exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202410 and ma_kpi='HCM_CL_DHQLY_007' and ma_vtcv='VNP-HNHCM_BHKV_52'
                 and ma_vtcv=a.ma_vtcv);
                 
update ct_bsc_cl_dhqly_007_bhol a
   set sl_dat=nvl((select sum(sl_dat) from ct_bsc_cl_dhqly_007_bhol where thang=a.thang and ma_to=a.ma_to and ma_vtcv<>a.ma_vtcv),0)
      ,sl_khongdat=(select sum(sl_khongdat) from ct_bsc_cl_dhqly_007_bhol where thang=a.thang and ma_to=a.ma_to and ma_vtcv<>a.ma_vtcv)
 where thang=202410 
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202410 and ma_kpi='HCM_CL_DHQLY_007' and ma_vtcv='VNP-HNHCM_BHKV_52'
                 and ma_vtcv=a.ma_vtcv);
update ct_bsc_cl_dhqly_007_bhol a
   set tyle_thuchien=round(nvl(sl_dat,0)/(nvl(sl_dat,0)+nvl(sl_khongdat,0))*100,2)
 where thang=202410 
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202410 and ma_kpi='HCM_CL_DHQLY_007' and ma_vtcv='VNP-HNHCM_BHKV_52'
                 and ma_vtcv=a.ma_vtcv)
   and (nvl(sl_dat,0)+nvl(sl_khongdat,0))>0;

update ct_bsc_cl_dhqly_007_bhol a
   set diem_cong=case when tyle_thuchien=100 and mdo_hthanh_dthu_ptm>=100 then 20 end 
      ,diem_tru=case when tyle_thuchien<50 then 20 end 
 where thang=202410 
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202410 and ma_kpi='HCM_CL_DHQLY_007' and ma_vtcv='VNP-HNHCM_BHKV_52'
                 and ma_vtcv=a.ma_vtcv);                                  
 

update bangluong_kpi a
   set (diem_cong,diem_tru)=(select diem_cong,diem_tru 
                               from ct_bsc_cl_dhqly_007_bhol 
                              where thang=a.thang and ma_nv=a.ma_nv
                                and ma_nv=ma_nhanvien)
      -- update bangluong_kpi a set diem_cong='',diem_tru='' 
      --    select * from bangluong_kpi a                           
 where thang=202410 and ma_kpi='HCM_CL_DHQLY_007'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)=upper(a.ma_kpi) and ma_vtcv='VNP-HNHCM_BHKV_52' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);
 ; 


----------------------------------------HCM_CL_DHQLY_006 -- TT

delete from ct_bsc_cl_dhqly_006_tt where thang=202410;
insert into ct_bsc_cl_dhqly_006_tt
select thang,ma_pb,ten_pb,ma_to,ten_to
      ,(select ma_nv from nhanvien nv 
         where thang=a.thang and ma_to=a.ma_to 
           and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=a.thang and ma_kpi=a.ma_kpi and to_truong_pho is not null 
                 and ma_vtcv=nv.ma_vtcv)
         )ma_nv
      ,ma_nv ma_nhanvien,ten_nv ten_nhanvien,ma_vtcv,ten_vtcv,mucdo_hoanthanh
      ,case when mucdo_hoanthanh>=100 then 1 end sl_dat,case when mucdo_hoanthanh<100 then 1 end sl_khongdat
      ,cast(null as number)tyle_thuchien,cast(null as number)diem_cong,cast(null as number)diem_tru
  from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_DT_PTMOI_021'
   and exists(select x.ma_to  
                from nhanvien x,blkpi_danhmuc_kpi_vtcv y
               where x.thang=y.thang and x.ma_vtcv=y.ma_vtcv
                 and y.ma_kpi='HCM_CL_DHQLY_006' and y.to_truong_pho is not null and y.ma_vtcv<>'VNP-HNHCM_BHKV_17'
                 and x.thang=a.thang and x.ma_to=a.ma_to)
   and exists(select * from nhanvien where thang=a.thang and tinh_bsc=1 and thaydoi_vtcv=0 and ma_nv=a.ma_nv)
   order by  ma_to,ma_vtcv;

                 
update ct_bsc_cl_dhqly_006_tt a
   set sl_dat=case when mucdo_hoanthanh>=100 then 1 end
      ,sl_khongdat=case when mucdo_hoanthanh<100 then 1 end
 where thang=202410 
   and not exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=a.thang and ma_kpi='HCM_CL_DHQLY_006' 
                 and (to_truong_pho is not null or giamdoc_phogiamdoc is not null)
                 and ma_vtcv=a.ma_vtcv);
                                 
update ct_bsc_cl_dhqly_006_tt a
   set sl_dat=(select nvl(sum(sl_dat),0) from ct_bsc_cl_dhqly_006_tt where thang=a.thang and ma_to=a.ma_to and ma_vtcv<>a.ma_vtcv)
      ,sl_khongdat=(select nvl(sum(sl_khongdat),0) from ct_bsc_cl_dhqly_006_tt where thang=a.thang and ma_to=a.ma_to and ma_vtcv<>a.ma_vtcv)
 where thang=202410 
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=a.thang and ma_kpi='HCM_CL_DHQLY_006' and to_truong_pho is not null
                 and ma_vtcv=a.ma_vtcv);                 
                 
update ct_bsc_cl_dhqly_006_tt a
   set tyle_thuchien=round(sl_dat/(nvl(sl_dat,0)+nvl(sl_khongdat,0))*100,2)
 where thang=202410 
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=a.thang and ma_kpi='HCM_CL_DHQLY_006' and to_truong_pho is not null
                 and ma_vtcv=a.ma_vtcv)
   and (nvl(sl_dat,0)+nvl(sl_khongdat,0))>0;                
                 
update ct_bsc_cl_dhqly_006_tt a
   set diem_cong=case when tyle_thuchien=100 and mucdo_hoanthanh>=100 then 20 end 
      ,diem_tru=case when tyle_thuchien<50 then 20 end 
 where thang=202410 
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202410 and ma_kpi='HCM_CL_DHQLY_006' and to_truong_pho is not null
                 and ma_vtcv=a.ma_vtcv)
   and exists(select * from nhanvien where thang=a.thang and tinh_bsc=1 and thaydoi_vtcv=0 and ma_nv=a.ma_nv)
;                              
     
update bangluong_kpi a
   set (diem_cong,diem_tru)=(select diem_cong,diem_tru 
                               from ct_bsc_cl_dhqly_006_tt 
                              where thang=a.thang and ma_nv=a.ma_nv
                                and ma_nv=ma_nhanvien)
      -- update bangluong_kpi set tyle_thuchien=''   
      --    select * from bangluong_kpi a                          
 where thang=202410 and ma_kpi='HCM_CL_DHQLY_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)=upper(a.ma_kpi) and to_truong_pho is not null and ma_vtcv<>'VNP-HNHCM_BHKV_17'
                  and ma_vtcv=a.ma_vtcv);
 ; 



----------------------------------------HCM_CL_DHQLY_006 -- PGD

delete from ct_bsc_cl_dhqly_006_pgd where thang=202410;
insert into ct_bsc_cl_dhqly_006_pgd
select 202410 thang,ma_pb,ma_nv,ma_nhanvien
      ,sum(nhombrcd_dtgiao)nhombrcd_dtgiao,sum(nhombrcd_kqth)nhombrcd_kqth
      ,sum(nhomvina_dtgiao)nhomvina_dtgiao,sum(nhomvina_kqth)nhomvina_kqth
      ,sum(nhomcntt_dtgiao)nhomcntt_dtgiao,sum(nhomcntt_kqth)nhomcntt_kqth
      ,sum(nhomtsl_dtgiao)nhomtsl_dtgiao,sum(nhomconlai_kqth)nhomconlai_kqth
      ,null tong_dthu_giao,null tong_dthu_thuchien,null tyle_thuchien,null mucdo_hoanthanh
      ,null sl_dat,null sl_khongdat,null tyle_dat,null diem_cong,null diem_tru    
  from(
  
select ma_pb,(select distinct ma_nv from blkpi_dm_to_pgd where thang=a.thang and dichvu in ('Mega+Fiber','MyTV') and ma_to=a.ma_to)ma_nv,ma_nv ma_nhanvien
      ,nhombrcd_dtgiao,nhombrcd_kqth,null nhomvina_dtgiao,null nhomvina_kqth
      ,null nhomcntt_dtgiao,null nhomcntt_kqth,null nhomtsl_dtgiao,null nhomconlai_kqth     
  from dinhmuc_giao_dthu_ptm a
 where thang=202410 and nhombrcd_dtgiao>0
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=a.thang and upper(ma_kpi)='HCM_DT_PTMOI_021' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv)
   and exists(select * from nhanvien where thang=a.thang and tinh_bsc=1 and thaydoi_vtcv=0 and ma_nv=a.ma_nv)
                  
 union all
select ma_pb,(select distinct ma_nv from blkpi_dm_to_pgd where thang=a.thang and dichvu in('VNP tra sau','VNP tra truoc') and ma_to=a.ma_to)ma_nv,ma_nv ma_nhanvien
      ,null nhombrcd_dtgiao,null nhombrcd_kqth,nhomvina_dtgiao,nvl(nhomvinats_kqth,0)+nvl(nhomvinatt_kqth,0)nhomvina_kqth
      ,null nhomcntt_dtgiao,null nhomcntt_kqth,null nhomtsl_dtgiao,null nhomconlai_kqth
  from dinhmuc_giao_dthu_ptm a
 where thang=202410 and nhomvina_dtgiao>0
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=a.thang and upper(ma_kpi)='HCM_DT_PTMOI_021' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv)
   and exists(select * from nhanvien where thang=a.thang and tinh_bsc=1 and thaydoi_vtcv=0 and ma_nv=a.ma_nv)
   
 union all
select ma_pb,(select ma_nv from blkpi_dm_to_pgd where thang=a.thang and dichvu='Dich vu so doanh nghiep' and ma_to=a.ma_to)ma_nv,ma_nv ma_nhanvien
      ,null nhombrcd_dtgiao,null nhombrcd_kqth,null nhomvina_dtgiao,null nhomvina_kqth
      ,nhomcntt_dtgiao,nhomcntt_kqth,null nhomtsl_dtgiao,null nhomconlai_kqth 
    --  select *
  from dinhmuc_giao_dthu_ptm a
 where thang=202410 and nhomcntt_dtgiao>0
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=a.thang and upper(ma_kpi)='HCM_DT_PTMOI_021' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv)
   and exists(select * from nhanvien where thang=a.thang and tinh_bsc=1 and thaydoi_vtcv=0 and ma_nv=a.ma_nv)
                  
 union all
select ma_pb,(select ma_nv from blkpi_dm_to_pgd where thang=a.thang and dichvu='Con lai' and ma_to=a.ma_to)ma_nv,ma_nv ma_nhanvien
      ,null nhombrcd_dtgiao,null nhombrcd_kqth,null nhomvina_dtgiao,null nhomvina_kqth
      ,null nhomcntt_dtgiao,null nhomcntt_kqth,nhomtsl_dtgiao,nhomconlai_kqth
    --  select *
  from dinhmuc_giao_dthu_ptm a
 where thang=202410 and nhomtsl_dtgiao>0
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=a.thang and upper(ma_kpi)='HCM_DT_PTMOI_021' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv)               
   and exists(select * from nhanvien where thang=a.thang and tinh_bsc=1 and thaydoi_vtcv=0 and ma_nv=a.ma_nv)
                  
)where ma_nv is not null
 group by ma_pb,ma_nv,ma_nhanvien;


update ct_bsc_cl_dhqly_006_pgd
   set tong_dthu_giao=nvl(nhombrcd_dtgiao,0)+nvl(nhomvina_dtgiao,0)+nvl(nhomcntt_dtgiao,0)+nvl(nhomtsl_dtgiao,0)
      ,tong_dthu_thuchien=nvl(nhombrcd_kqth,0)+nvl(nhomvina_kqth,0)+nvl(nhomcntt_kqth,0)+nvl(nhomconlai_kqth,0)
 where thang=202410;

update ct_bsc_cl_dhqly_006_pgd
   set tyle_thuchien=round(tong_dthu_thuchien/tong_dthu_giao*100,2)
 where thang=202410 and tong_dthu_giao>0;
 
update ct_bsc_cl_dhqly_006_pgd
   set mucdo_hoanthanh=case when tyle_thuchien<30 then 0        -- 0%
                            when tyle_thuchien>=30 and tyle_thuchien<=70
                                then round(0.85*tyle_thuchien,2)          -- 85% *TLTH --> chi Vuong Viber y/c delete * ty trong
                            when tyle_thuchien>70 and tyle_thuchien<=100
                                then round(1*tyle_thuchien,2)                -- 100% *TLTH --> chi Vuong Viber y/c delete * ty trong
                            when tyle_thuchien>100
                                then case when 100+(1.2*(tyle_thuchien-100))>150 then 150
                                          else round(100+(1.2*(tyle_thuchien-100)),2) ---100% + 1.2 x (TLTH – 100%) -- max 150%
                                      end
                        end
 where thang=202410; 

update ct_bsc_cl_dhqly_006_pgd
   set sl_dat=case when mucdo_hoanthanh>=100 then 1 end,sl_khongdat=case when mucdo_hoanthanh<100 then 1 end
 where thang=202410;


insert into ct_bsc_cl_dhqly_006_pgd(thang,ma_pb,ma_nv,ma_nhanvien,mucdo_hoanthanh)
select thang,ma_pb,ma_nv,' TONG CONG'ma_nhanvien
      ,(select mucdo_hoanthanh from bangluong_kpi where thang=a.thang and ma_kpi='HCM_DT_PTMOI_021' and ma_nv=a.ma_nv)
  from nhanvien a
 where thang=202410
   and exists(select * from ct_bsc_cl_dhqly_006_pgd where thang=a.thang and ma_nv=a.ma_nv)
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=a.thang and ma_kpi='HCM_CL_DHQLY_006' and giamdoc_phogiamdoc is not null
                 and ma_vtcv=a.ma_vtcv)
   and exists(select * from nhanvien where thang=a.thang and tinh_bsc=1 and thaydoi_vtcv=0 and ma_nv=a.ma_nv)              
   ;


update ct_bsc_cl_dhqly_006_pgd a
   set sl_dat=(select nvl(sum(sl_dat),0) from ct_bsc_cl_dhqly_006_pgd where thang=a.thang and ma_nhanvien<>' TONG CONG' and ma_nv=a.ma_nv)
      ,sl_khongdat=(select nvl(sum(sl_khongdat),0) from ct_bsc_cl_dhqly_006_pgd where thang=a.thang and ma_nhanvien<>' TONG CONG' and ma_nv=a.ma_nv)
 where thang=202410 and ma_nhanvien=' TONG CONG';   
 
update ct_bsc_cl_dhqly_006_pgd a
   set tyle_dat=round(nvl(sl_dat,0)/(nvl(sl_dat,0)+nvl(sl_khongdat,0))*100,2)
 where thang=202410 and ma_nhanvien=' TONG CONG';  

update ct_bsc_cl_dhqly_006_pgd a
   set diem_cong=case when tyle_dat=100 and mucdo_hoanthanh>=100 then 20 end 
      ,diem_tru=case when tyle_dat<50 then 20 end
 where thang=202410 and ma_nhanvien=' TONG CONG'; 
 


update bangluong_kpi a
   set (diem_cong,diem_tru)=(select diem_cong,diem_tru 
                               from ct_bsc_cl_dhqly_006_pgd
                              where thang=a.thang and ma_nhanvien=' TONG CONG' and ma_nv=a.ma_nv)
      -- update bangluong_kpi set tyle_thuchien=''
      --    select * from bangluong_kpi a                           
 where thang=202410 and ma_kpi='HCM_CL_DHQLY_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_CL_DHQLY_006' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);



----------------------------------------HCM_SL_HOTRO_001

delete from ct_nghiepvu_sbh where thang=202410;
insert into ct_nghiepvu_sbh
select distinct 202410 thang,ma_gd,ma_tb,ma_kh,loai,ten_loaihd,ngay_cn
      ,(select nhom_tinh from dm_nghiepvu_sbh where tinh=1 and loai=a.loai and ten_loaihd=a.ten_loaihd)nhom_tinh
      ,case when loai like '%ONEBSS%' and ma_gd is null then 'Khong co ma giao dich'
            when exists(select * from dm_nghiepvu_sbh where tinh=0 and loai=a.loai and ten_loaihd=a.ten_loaihd) then 'Nghiep vu khong tinh'
        end khong_tinh
      ,manv_ra_pct ma_nv
      ,(select ma_to from nhanvien where thang=202410 and ma_nv=a.manv_ra_pct)ma_to
      ,(select ma_pb from nhanvien where thang=202410 and ma_nv=a.manv_ra_pct)ma_pb
  from tuyenngo.SBH_GIAODICH_202410_ct_new1 a 
 where exists(select * from blkpi_danhmuc_kpi_vtcv where thang=202410 and ma_kpi='HCM_SL_HOTRO_001' and ma_vtcv=a.ma_vtcv)  
;


update ct_nghiepvu_sbh a set khong_tinh='Trung thao tac ben ccbs'
 where thang=202410
   and loai='USSD' and ten_loaihd='DOI SIM'
   and exists(select * from ct_nghiepvu_sbh where thang=202410 and loai='VNP - CCBS' and ten_loaihd='DOI SIM' and ma_tb=a.ma_tb);



drop table temp_sbh;
create table temp_sbh as

select ma_nv,ma_tb,''ma_gd,1 sl
  from ct_nghiepvu_sbh 
 where thang=202410 and khong_tinh is null and nhom_tinh=1
 
  union all
select ma_nv,ma_tb,ma_gd,least(count(*),2)sl
  from ct_nghiepvu_sbh 
 where thang=202410 and khong_tinh is null and nhom_tinh=2
 group by ma_nv,ma_tb,ma_gd
 
  union all
select ma_nv,ma_tb,ma_gd,least(count(*),1)sl
  from ct_nghiepvu_sbh 
 where thang=202410 and khong_tinh is null and nhom_tinh=3
 group by ma_nv,ma_tb,ma_gd 
 
 union all
select ma_nv,'','',count(distinct ma_kh)*0.25 sl
  from ct_nghiepvu_sbh a
 where thang=202410 and khong_tinh is null and nhom_tinh=5
 group by ma_nv
 ;


update bangluong_kpi a
   set thuchien=(select sum(sl) from temp_sbh where ma_nv=a.ma_nv) 
       --update bangluong_kpi a set thuchien=''
       --select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_SL_HOTRO_001'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_SL_HOTRO_001' and to_truong_pho is null
                  and ma_vtcv=a.ma_vtcv);


update bangluong_kpi a
   set tyle_thuchien=round(thuchien/giao*100,2)
      ,mucdo_hoanthanh=least(round(thuchien/giao*100,2),120)
       --   update bangluong_kpi a set tyle_thuchien='',mucdo_hoanthanh=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_SL_HOTRO_001';



----------------------------------------HCM_SL_CSKHH_003

drop table SBH_GIAODICH_202410_CT_new;
create table SBH_GIAODICH_202410_CT_new as 
select ma_gd,ma_tb,ma_kh,ten_loaihd,loai,nhomtinh,ngay_cn,rownum stt,cast(null as varchar(100))khong_tinh
      ,manv_ra_pct ma_nv
      ,(select ma_to from nhanvien where thang=202410 and ma_nv=b.manv_ra_pct)ma_to
      ,(select ma_pb from nhanvien where thang=202410 and ma_nv=b.manv_ra_pct)ma_pb 
  from(select distinct ma_gd,ma_tb,ma_kh,manv_ra_pct,ten_loaihd,loai,ngay_cn
             ,(select nhomtinh from dm_haumai_gdv where loai=a.loai and ten_loaihd=a.ten_loaihd)nhomtinh
         from tuyenngo.SBH_GIAODICH_202410_CT_new a
        where manv_ra_pct is not null
          and exists (select * from blkpi_danhmuc_kpi_vtcv 
                       where thang=202410 and upper(ma_kpi)='HCM_SL_CSKHH_003' and ma_vtcv=a.ma_vtcv)
        order by ten_loaihd,loai,ngay_cn)b;

update SBH_GIAODICH_202410_CT_new set khong_tinh='Khong co thong tin: ma giao dich, ma KH, ma thue bao'
 where ma_gd is null and ma_kh is null and ma_tb is null;

update SBH_GIAODICH_202410_CT_new set khong_tinh='thao tac tu ID163,164'
 where (ma_tb,ma_nv) in (select case when length(b.matb)=9 and lpad(b.matb,2) in ('81','82','83','84','85','88','91','94')
                                         then '84'||b.matb
                                     else b.matb end 
                               ,a.ma_nv
                           from ttkd_bsc.ktdt_ct_bsc_xl_nghievu_cskh a,ttkdhcm_ktnv.yckh_thuebao b
                          where a.thang=202410 and a.id_yckh=b.id_yckh);



drop table temp_giahan_tratruoc;
create table temp_giahan_tratruoc as
select distinct b.ma_tb,a.ma_gd
  from css.v_hd_khachhang@dataguard a
       join css.v_hd_thuebao@dataguard b on a.hdkh_id=b.hdkh_id
       join css.v_ct_tienhd@dataguard c on b.hdtb_id=c.hdtb_id
 where kieuld_id in (551,550,24,13080) and c.khoanmuctt_id=11 and c.tien>0 and to_char(ngay_yc,'yyyymm')='202410';
create index temp_giahan_tratruoc_1 on temp_giahan_tratruoc(ma_tb,ma_gd);

update SBH_GIAODICH_202410_CT_new a set khong_tinh='Trong tien trinh GHTT'
 where exists(select * from temp_giahan_tratruoc where ma_tb=a.ma_tb and ma_gd=a.ma_gd);
   
 

update SBH_GIAODICH_202410_CT_new a set khong_tinh='Trong tien trinh lap dat moi'
 where exists(select * from SBH_GIAODICH_202410_CT_new 
               where ten_loaihd in ('HMM TRA SAU','LAP DAT MOI') and ma_tb=a.ma_tb and ma_nv=a.ma_nv);
   

drop table loai_sms_dai;
create table loai_sms_dai as
select * FROM ttkdhcm_ktnv.gdv_haumai a
 where exists (select 1 
                 from (SELECT b.id_sms
                         FROM ttkdhcm_ktnv.gdv_haumai_ketqua_giamsat a
                              left join ttkdhcm_ktnv.gdv_haumai_kq b on b.goira_id=a.goira_id
                        where a.kq_nghe_file=7 and a.ma_loi=1) b 
                where b.id_sms=a.id_sms)
  and to_char(ngay_yc,'yyyymm')='202410';
  
  
update SBH_GIAODICH_202410_CT_new a set khong_tinh='KH co yc thao tac khac'
 where loai='VNP - CCBS' and ten_loaihd='THU CUOC'
   and exists(select * from SBH_GIAODICH_202410_CT_new where ten_loaihd not in ('THU CUOC','CAP NHAT DB') and ma_kh=a.ma_kh);
 
 
update SBH_GIAODICH_202410_CT_new a set khong_tinh='KH co yc thao tac khac'
 where loai='THU CUOC ONEBSS' and ten_loaihd='THU CUOC'
   and exists(select * from SBH_GIAODICH_202410_CT_new where ten_loaihd!='THU CUOC' and ma_kh=a.ma_kh);

update SBH_GIAODICH_202410_CT_new a set khong_tinh='KH da duoc nhan tin'
 where loai='THU CUOC ONEBSS' and ten_loaihd='THU CUOC'
   and exists(select * from ttkdhcm_ktnv.gdv_haumai where to_char(ngay_yc,'yyyymm')='202410' and no_sms=1 and ma_tb=a.ma_tb);

update SBH_GIAODICH_202410_CT_new a set khong_tinh='KH da duoc nhan tin (dai)'
 where loai='THU CUOC ONEBSS' and ten_loaihd='THU CUOC'
   and exists(select * from loai_sms_dai where ma_tb=a.ma_tb);
   
   
update SBH_GIAODICH_202410_CT_new a set khong_tinh='KH co yc thao tac khac'
 where loai='ONEBSS' and ten_loaihd='THU KHAC'
   and exists(select * from SBH_GIAODICH_202410_CT_new where ten_loaihd!='THU KHAC' and ma_kh=a.ma_kh);

update SBH_GIAODICH_202410_CT_new a set khong_tinh='KH da duoc nhan tin'
 where loai='ONEBSS' and ten_loaihd='THU KHAC'
   and exists(select * from ttkdhcm_ktnv.gdv_haumai where to_char(ngay_yc,'yyyymm')='202410' and no_sms=1 and ma_tb=a.ma_tb);

update SBH_GIAODICH_202410_CT_new a set khong_tinh='KH da duoc nhan tin (dai)'
 where loai='ONEBSS' and ten_loaihd='THU KHAC'
   and exists(select * from loai_sms_dai where ma_tb=a.ma_tb);


update SBH_GIAODICH_202410_CT_new a set khong_tinh='Trung khi dong bo doi sim ve ccbs'
  --select * from SBH_GIAODICH_202410_CT_new a
 where loai='USSD' and ten_loaihd='DOI SIM'
   and exists(select * from SBH_GIAODICH_202410_CT_new where loai='VNP - CCBS' and ten_loaihd='DOI SIM' and ma_tb=a.ma_tb);



update SBH_GIAODICH_202410_CT_new a set khong_tinh='Cap nhat DB da dong bo ben Nghiep vu khac'
 where loai='VNP - CCBS' and ten_loaihd='CAP NHAT DB'
   and exists(select * from SBH_GIAODICH_202410_CT_new 
               where loai='VNP - CCBS' and ten_loaihd<>'CAP NHAT DB' 
                 and ma_tb=a.ma_tb);-- and ma_nv=a.ma_nv);

commit;


------- Chuyen doi VCC
alter table SBH_GIAODICH_202410_CT_new add chuyen_vcc varchar(100);

update SBH_GIAODICH_202410_CT_new a 
   set chuyen_vcc=1
        --select * from SBH_GIAODICH_202410_CT_new a
 where exists(select * from ttkd_bsc.dt_ptm_vnp_202410_bs where somay=a.ma_tb)
   and ten_loaihd='DANG KY HUY CHUYEN DOI GOI CUOC';
                    
                    
select * from SBH_GIAODICH_202410_CT_new 
 where ten_loaihd='DANG KY HUY CHUYEN DOI GOI CUOC' 
   and chuyen_vcc is not null;


select distinct tenkieu_ld from ttkd_bsc.ct_bsc_ptm 
                    where thang_ptm=202410 and loaitb_id=149


alter table SBH_GIAODICH_202410_CT_new add ten_tb varchar(400);
alter table SBH_GIAODICH_202410_CT_new add ma_dt_kh varchar(2);

update SBH_GIAODICH_202410_CT_new a
   set (ten_tb,ma_dt_kh)=(select ten_tb,ma_dt_kh from ttkd_bct.db_thuebao_ttkd where ma_tb=a.ma_tb and ma_tt=a.ma_kh);
   


drop table tmp_ct_gdv;
create table tmp_ct_gdv as

select ma_to,ma_nv,count(distinct ma_kh)*0.25 sl,loai,ten_loaihd
  from SBH_GIAODICH_202410_CT_new a
 where loai='VNP - CCBS' and ten_loaihd='THU CUOC'
   and khong_tinh is null
 group by ma_to,ma_nv,loai,ten_loaihd
   
 union all

select ma_to,ma_nv,count(distinct ma_kh)*0.25 sl,loai,ten_loaihd
  from SBH_GIAODICH_202410_CT_new a
 where loai='THU CUOC ONEBSS' and ten_loaihd='THU CUOC'
   and khong_tinh is null
 group by ma_to,ma_nv,loai,ten_loaihd
 
  union all

select ma_to,ma_nv,count(distinct ma_kh)*0.25 sl,loai,ten_loaihd
  from SBH_GIAODICH_202410_CT_new a
 where loai='ONEBSS' and ten_loaihd='THU KHAC'
   and khong_tinh is null
 group by ma_to,ma_nv,loai,ten_loaihd
                   
 union all
select ma_to,ma_nv,count(*)sl,loai,ten_loaihd
  from SBH_GIAODICH_202410_CT_new a
 where nhomtinh=1 
   and khong_tinh is null
 group by ma_to,ma_nv,loai,ten_loaihd
         
 union all
select ma_to,ma_nv,count(*)sl,loai,ten_loaihd
  from (select * from SBH_GIAODICH_202410_CT_new where khong_tinh is null)a
 where nhomtinh=2 -- nhom 2: lay thao tac cuoi cung trong ngay
   and stt=(select max(stt) from SBH_GIAODICH_202410_CT_new 
             where nhomtinh=2 
               and loai=a.loai and ten_loaihd=a.ten_loaihd and ma_tb=a.ma_tb 
               and to_char(ngay_cn,'yyyymmdd')=to_char(a.ngay_cn,'yyyymmdd')
           )       
 group by ma_to,ma_nv,loai,ten_loaihd   
 
  union all

select ma_to,ma_nv,count(*)sl,loai,ten_loaihd
  from (select * from SBH_GIAODICH_202410_CT_new where khong_tinh is null)a
 where nhomtinh=3 -- nhom 3: lay thao tac cuoi cung trong thang
   and stt=(select max(stt) from SBH_GIAODICH_202410_CT_new 
             where nhomtinh=3 
               and loai=a.loai and ten_loaihd=a.ten_loaihd and ma_tb=a.ma_tb 
               and to_char(ngay_cn,'yyyymm')=to_char(a.ngay_cn,'yyyymm')
           )
 group by ma_to,ma_nv,loai,ten_loaihd 
;    
 




update bangluong_kpi a
   set thuchien=(select sum(sl)*1.1 from tmp_ct_gdv where ma_nv=a.ma_nv)
       --   update bangluong_kpi set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_SL_CSKHH_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_SL_CSKHH_003' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
 
 
 
update bangluong_kpi a
   set thuchien=(select sum(sl)*1.1 from tmp_ct_gdv where ma_to=a.ma_to)
       --   update bangluong_kpi a set thuchien=''
       --   select sum(giao),count(*) from bangluong_kpi a
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_SL_CSKHH_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202410 and upper(ma_kpi)='HCM_SL_CSKHH_003' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);
                                    

update bangluong_kpi a
   set tyle_thuchien=round(thuchien/giao*100,2)
      ,mucdo_hoanthanh=least(round(thuchien/giao*100,2),120)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202410 and ma_kpi='HCM_SL_CSKHH_003'
