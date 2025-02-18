
----------------------------------------HCM_SL_DAILY_003

delete from ptm_daily_cops_dthu where thang_ptm=202412;
insert into ptm_daily_cops_dthu
select thang_ptm,ma_tb,ma_gd,ten_tb,dthu_goi,ma_nguoigt,cast(null as varchar(10))duoctinh,ma_pb,ma_to,manv_ptm ma_nv,thang_ptm thang
         --,trangthaitb_id,chuquan_id
 from ttkd_bsc.ct_bsc_ptm a
 where thang_ptm=202412
   and exists(select * from ttkd_bsc.dm_daily_khdn where thang=202412 and loai_hopdong='DAI LY MOI' and ma_daily=a.ma_nguoigt)
   and trangthaitb_id=1 and chuquan_id=145
   and exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv where thang=202412 and ma_kpi='HCM_SL_DAILY_003' and ma_vtcv=a.ma_vtcv);

update ptm_daily_cops_dthu a set duoctinh='duoc tinh'
 where thang_ptm=202412
   and exists(select ma_nguoigt  
                from ptm_daily_cops_dthu
               where thang_ptm=a.thang_ptm and ma_nguoigt=a.ma_nguoigt
               group by ma_nguoigt
              having sum(dthu_goi)>=3000000);
commit;



update bangluong_kpi a
   set thuchien=nvl((select count(distinct ma_nguoigt) from ptm_daily_cops_dthu 
                      where duoctinh='duoc tinh' and ma_nv=a.ma_nv and thang_ptm=a.thang)
                    ,0)  
       --select * from bangluong_kpi a
       --update bangluong_kpi a set thuchien=''
 where thang=202412 and ma_kpi='HCM_SL_DAILY_003'
   and exists(select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
               where thang=202412 and ma_kpi='HCM_SL_DAILY_003'
                 and to_truong_pho is null and giamdoc_phogiamdoc is null
                 and ma_vtcv=a.ma_vtcv);
  

update bangluong_kpi a
   set thuchien=nvl((select count(distinct ma_nguoigt) from ptm_daily_cops_dthu 
                      where duoctinh='duoc tinh' and ma_to=a.ma_to and thang_ptm=a.thang)
                    ,0)  
       --select * from bangluong_kpi a
       --update bangluong_kpi a set thuchien=''
 where thang=202412 and ma_kpi='HCM_SL_DAILY_003'
   and exists(select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
               where thang=202412 and ma_kpi='HCM_SL_DAILY_003'
                 and to_truong_pho is not null
                 and ma_vtcv=a.ma_vtcv);
 
   

update bangluong_kpi a
   set tyle_thuchien=round(thuchien/giao*100,2)
      ,mucdo_hoanthanh=least(round(thuchien/giao*100,2),120)
       --select * from bangluong_kpi a
       --update bangluong_kpi a set thuchien=''
 where thang=202412 and ma_kpi='HCM_SL_DAILY_003' and giao>0;
 

----------------------------------------HCM_DT_PTMOI_056
--create table nguyennc.ct_bsc_ptmoi_056_202412_l1 as select * from ct_bsc_ptmoi_056 where thang=202412;

delete from ct_bsc_ptmoi_056 where thang=202412;
insert into ct_bsc_ptmoi_056
       (thang,ma_pb,ma_to,ma_nv,ma_vtcv
       ,cntt_dthu_giao,cntt_dthu_thuchien,cdbr_mytv_dthu_giao,cdbr_mytv_dthu_thuchien,didong_dthu_giao,didong_dthu_thuchien)   
select 202412,ma_pb,ma_to,ma_nv,ma_vtcv
      ,nhomcntt_dtgiao,nhomcntt_kqth,nhombrcd_dtgiao,nhombrcd_kqth
      ,nhomvina_dtgiao,nvl(nhomvinats_kqth,0)+nvl(nhomvinatt_kqth,0)nhomvina_kqth
  from dinhmuc_giao_dthu_ptm a
 where thang=202412
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202412 and ma_kpi='HCM_DT_PTMOI_056' and to_truong_pho is null and giamdoc_phogiamdoc is null 
                 and ma_vtcv=a.ma_vtcv)
;
   
update ct_bsc_ptmoi_056 a
   set cntt_tyle_thuchien=case when cntt_dthu_giao>0 then round(nvl(cntt_dthu_thuchien,0)/cntt_dthu_giao*100,2) end
      ,cntt_diem_tru_bsc=case when cntt_dthu_giao>0 and nvl(cntt_dthu_thuchien,0)<cntt_dthu_giao*0.9 then 5 end
      ,cdbr_mytv_tyle_thuchien=case when cdbr_mytv_dthu_giao>0 then round(nvl(cdbr_mytv_dthu_thuchien,0)/cdbr_mytv_dthu_giao*100,2) end
      ,cdbr_mytv_diem_tru_bsc=case when cdbr_mytv_dthu_giao>0 and nvl(cdbr_mytv_dthu_thuchien,0)<cdbr_mytv_dthu_giao*0.9 then 5 end
      ,didong_tyle_thuchien=case when didong_dthu_giao>0 then round(nvl(didong_dthu_thuchien,0)/didong_dthu_giao*100,2) end
      ,didong_diem_tru_bsc=case when didong_dthu_giao>0 and nvl(didong_dthu_thuchien,0)<didong_dthu_giao*0.9 then 5 end   
 where thang=202412;

update ct_bsc_ptmoi_056 a
   set tong_diem_tru_bsc=nvl(cntt_diem_tru_bsc,0)+nvl(cdbr_mytv_diem_tru_bsc,0)+nvl(didong_diem_tru_bsc,0)
 where thang=202412;   
 


update bangluong_kpi a
   set diem_tru=(select tong_diem_tru_bsc from ct_bsc_ptmoi_056 where thang=a.thang and ma_nv=a.ma_nv)
       --update bangluong_kpi a set diem_tru=''
       --select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_PTMOI_056'
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202412 and ma_kpi='HCM_DT_PTMOI_056' and to_truong_pho is null and giamdoc_phogiamdoc is null
                 and ma_vtcv=a.ma_vtcv)
   and exists(select tong_diem_tru_bsc from ct_bsc_ptmoi_056 where thang=a.thang and ma_nv=a.ma_nv);
 


update bangluong_kpi a set diem_tru=0                         
 where thang=202412 and ma_kpi='HCM_DT_PTMOI_056'
   and diem_tru is null;                
                 
----------------------------------------HCM_DT_PTMOI_054

select a.*,(select dthu_goi from ct_bsc_ptm where id=a.id)dthu_goi_new,dthu_goi-(select dthu_goi from ct_bsc_ptm where id=a.id)
  from ct_dvs_pkhdn_2024 a
 where dthu_goi<>(select dthu_goi from ct_bsc_ptm where id=a.id)
 
update ct_dvs_pkhdn_2024 a
   set ghichu='d/c dthu_goi tu ky 202412, dthu_goi_old='||dthu_goi
      ,dthu_goi=(select dthu_goi from ct_bsc_ptm where id=a.id)
 where dthu_goi<>(select dthu_goi from ct_bsc_ptm where id=a.id);
 
 
delete from ct_dvs_pkhdn_2024 where thang_ptm=202412;
insert into ct_dvs_pkhdn_2024
select thang_ptm,ma_gd,ma_tb,ma_kh,ten_tb,dich_vu
      ,(select pdh_nhom_dvu_khdn_2024 from dm_loaihinh_hsqd where loaitb_id=a.loaitb_id)nhom_dichvu
      ,dthu_goi
      ,(select trangthai_tb from css_hcm.trangthai_tb where trangthaitb_id=a.trangthaitb_id)trangthai_tb
      ,(select tenchuquan from css_hcm.chuquan where chuquan_id=a.chuquan_id)chuquan,lydo_khongtinh_luong
      ,case when (trangthaitb_id=1 or trangthaitb_id is null or (loaitb_id in (20,149) and trangthaitb_id=10))
                 and chuquan_id in (145,264)
                then 1 end duoc_tinh
      ,ma_pb,ma_to,manv_ptm ma_nv,id,''ghichu
  from ttkd_bsc.ct_bsc_ptm a
 where thang_ptm=202412 and dthu_goi<>0
   and exists(select * from dm_loaihinh_hsqd where pdh_nhom_dvu_khdn_2024 is not null and loaitb_id=a.loaitb_id)
   --and dthu_goi<>(select dthu_goi from ttkd_bsc.ct_bsc_ptm where id=a.id)--kiem tra
;



update bangluong_kpi a
   set thuchien=(select nvl(round(sum(dthu_goi)/1000000,3),0) from ct_dvs_pkhdn_2024 where duoc_tinh=1 and ma_nv=a.ma_nv) 
       --update bangluong_kpi a set thuchien=''
       --select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_PTMOI_054'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_PTMOI_054' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);

                                   
update bangluong_kpi a
   set thuchien=(select round(sum(dthu_goi)/1000000,3) from ct_dvs_pkhdn_2024 where duoc_tinh=1 and ma_to=a.ma_to) 
      --update bangluong_kpi a set thuchien=''
      --    select * from bangluong_kpi a                         
 where thang=202412 and ma_kpi='HCM_DT_PTMOI_054'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_PTMOI_054' and to_truong_pho is not null
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
 where thang=202412 and ma_kpi='HCM_DT_PTMOI_054'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_PTMOI_054' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv)
   and ma_to in ('VNP0702406','VNP0702416'); -- SBH, SME4 
 
                  
drop table temp_bsc;
create table temp_bsc as
select dthu_goi
      ,(select ma_nv from blkpi_dm_to_pgd 
         where thang=202412 and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_kpi='HCM_DT_PTMOI_054' and ma_to=a.ma_to)ma_nv
  from ct_dvs_pkhdn_2024 a
 where duoc_tinh=1
 
 union all
select dthu_goi
      ,(select ma_nv from blkpi_dm_to_pgd 
         where thang=202412 and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_kpi='HCM_DT_PTMOI_054'
           and ma_to=decode(a.ma_to,'VNP0702413','VNP0702406','VNP0702416'))ma_nv
  from ct_dvs_pkhdn_2024 a
 where duoc_tinh=1 and thang_ptm<=202407 and ma_to in ('VNP0702413','VNP0702414')
;

update bangluong_kpi a
   set thuchien=(select round(sum(dthu_goi)/1000000,3) from temp_bsc where ma_nv=a.ma_nv) 
      -- update bangluong_kpi a set thuchien=''
      --    select * from bangluong_kpi a                            
 where thang=202412 and ma_kpi='HCM_DT_PTMOI_054'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_PTMOI_054' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);


update bangluong_kpi 
   set tyle_thuchien=least(round(thuchien/giao*100,2),120)
      ,mucdo_hoanthanh=least(round(thuchien/giao*100,2),120)
      -- update bangluong_kpi set tyle_thuchien=''
      --    select * from bangluong_kpi                            
 where thang=202412 and ma_kpi='HCM_DT_PTMOI_054';
 
 
 
----------------------------------------HCM_DT_LUYKE_002
  
update bangluong_kpi a
   set thuchien=(select round(sum(th_luyke_tong)/1000000,3) from nguyennc.ktkh_bcao_9a_th 
                  where thang=a.thang and stt='0' and ma_nv=a.ma_nv group by ma_nv)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_LUYKE_002' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
                  

update bangluong_kpi a
   set thuchien=(select round(sum(th_luyke_tong)/1000000,3) from nguyennc.ktkh_bcao_9a_th 
                  where thang=a.thang and stt='0' and ma_to=a.ma_to)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_LUYKE_002' and to_truong_pho is not null
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
 where thang=202412 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_LUYKE_002' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv)
   and ma_to in ('VNP0702406','VNP0702416');
                  
 

drop table temp_bsc_thuchien;
create table temp_bsc_thuchien as
select ma_nv,round(sum(th_luyke_tong)/1000000,3)dthu from (
select (select distinct ma_nv from blkpi_dm_to_pgd 
         where thang=a.thang and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_kpi='HCM_DT_LUYKE_002'
           and ma_to=a.ma_to)ma_nv
      ,th_luyke_tong
  from nguyennc.ktkh_bcao_9a_th a
 where thang=202412 and stt='0'
)group by ma_nv;

 
update bangluong_kpi a
   set thuchien=(select dthu from temp_bsc_thuchien where ma_nv=a.ma_nv)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_LUYKE_002'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_LUYKE_002' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);
                  
          
                              

update bangluong_kpi a set tyle_thuchien=round(thuchien/giao*100,2)
       --   update bangluong_kpi set tyle_thuchien=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_LUYKE_002';


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
 where thang=202412 and ma_kpi='HCM_DT_LUYKE_002';





-------HCM_DT_LUYKE_003
  
update bangluong_kpi a
   set thuchien=(select round(sum(th_luyke_tong)/1000000,3) from nguyennc.ktkh_bcao_9a_th where thang=a.thang and stt='1.1' and ma_nv=a.ma_nv)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_LUYKE_003' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
                  
 
update bangluong_kpi a
   set thuchien=(select round(sum(th_luyke_tong)/1000000,3) from nguyennc.ktkh_bcao_9a_th where thang=a.thang and stt='1.1' and ma_to=a.ma_to)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_LUYKE_003' and to_truong_pho is not null
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
 where thang=202412 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_LUYKE_003' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv)
   and ma_to in ('VNP0702406','VNP0702416');
                   
 
drop table temp_bsc_thuchien;
create table temp_bsc_thuchien as
select ma_nv,round(sum(th_luyke_tong)/1000000,3)dthu from (
select (select distinct ma_nv from blkpi_dm_to_pgd 
         where thang=a.thang and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_kpi='HCM_DT_LUYKE_003'--kiem tra ma nay co ko
           and ma_to=a.ma_to)ma_nv
      ,th_luyke_tong
  from nguyennc.ktkh_bcao_9a_th a
 where thang=202412 and stt='1.1'
)group by ma_nv;


update bangluong_kpi a
   set thuchien=(select dthu from temp_bsc_thuchien where ma_nv=a.ma_nv)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_LUYKE_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_LUYKE_003' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);
                     
 
   
update bangluong_kpi a set tyle_thuchien=round(thuchien/giao*100,2)
       --   update bangluong_kpi set tyle=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_LUYKE_003' and giao>0;
 
 
 
update bangluong_kpi a 
   set mucdo_hoanthanh=least(case when tyle_thuchien<30 then 0 else 1.1*tyle_thuchien end,150)     
       --   update bangluong_kpi set tyle=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_LUYKE_003';


 
 
----------------HCM_DT_PTNAM_006

update bangluong_kpi a
   set thuchien=(select round(sum(th_luyke_ptm)/1000000,3) from nguyennc.ktkh_bcao_9a_th where thang=a.thang and stt='0' and ma_nv=a.ma_nv)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_PTNAM_006' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
                  
 
update bangluong_kpi a
   set thuchien=(select round(sum(th_luyke_ptm)/1000000,3) from nguyennc.ktkh_bcao_9a_th where thang=a.thang and stt='0' and ma_to=a.ma_to)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_PTNAM_006' and to_truong_pho is not null
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
 where thang=202412 and ma_kpi='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_DT_PTNAM_006' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv)
   and ma_to in ('VNP0702406','VNP0702416');
                  
                   

drop table temp_bsc_thuchien;
create table temp_bsc_thuchien as
select ma_nv,round(sum(th_luyke_ptm)/1000000,3)dthu 
  from(select (select distinct ma_nv from blkpi_dm_to_pgd 
                where thang=202412 and ma_pb in ('VNP0702300','VNP0702400','VNP0702500') and ma_kpi='HCM_DT_PTNAM_006'
                  and ma_to=a.ma_to)ma_nv
             ,th_luyke_ptm
         from nguyennc.ktkh_bcao_9a_th a
        where thang=202412 and stt='0'
)group by ma_nv;

 
update bangluong_kpi a
   set thuchien=(select dthu from temp_bsc_thuchien where ma_nv=a.ma_nv)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202412 and upper(ma_kpi)='HCM_DT_PTNAM_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=a.thang and upper(ma_kpi)=upper(a.ma_kpi) and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);
                  
             
      

update bangluong_kpi a set tyle_thuchien=round(thuchien/giao*100,2)
       --   update bangluong_kpi set tyle=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_PTNAM_006' and giao>0;
 
 
update bangluong_kpi a 
   set mucdo_hoanthanh=round(least(case when tyle_thuchien<30 then 0 else 1.25*tyle_thuchien end,150),2)     
       --   update bangluong_kpi set tyle=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_DT_PTNAM_006';
  
 
 

----------------------------------------HCM_CL_DHQLY_007
--create table ct_bsc_cl_dhqly_007_bhol_202412_l1 as select * from ct_bsc_cl_dhqly_007_bhol where thang=202412;

delete from ct_bsc_cl_dhqly_007_bhol where thang=202412;
insert into ct_bsc_cl_dhqly_007_bhol
select distinct a.thang,a.ma_pb,a.ten_pb,a.ma_to,a.ten_to,b.ma_nv,a.ma_nv ma_nhanvien,a.ten_nv ten_nhanvien,a.ma_vtcv,a.ten_vtcv
      ,(select mucdo_hoanthanh from bangluong_kpi where thang=a.thang and upper(ma_kpi)='HCM_DT_PTMOI_021' and ma_nv=a.ma_nv)mdo_hthanh_dthu_ptm
      ,cast(null as number)sl_dat,cast(null as number)sl_khongdat,cast(null as number)tyle_thuchien
      ,cast(null as number)diem_cong,cast(null as number)diem_tru
  from nhanvien a,nhanvien b
 where a.thang=202412 and a.tinh_bsc=1 and a.thaydoi_vtcv=0 and a.thang=b.thang and a.ma_to=b.ma_to
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202412 and ma_kpi='HCM_CL_DHQLY_007' and ma_vtcv='VNP-HNHCM_BHKV_52'
                 and ma_vtcv=b.ma_vtcv)
 order by a.ma_to,a.ma_vtcv desc,a.ma_nv;

update ct_bsc_cl_dhqly_007_bhol a
   set sl_dat=case when mdo_hthanh_dthu_ptm>=100 then 1 end
      ,sl_khongdat=case when mdo_hthanh_dthu_ptm<100 then 1 end
 where thang=202412 
   and not exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202412 and ma_kpi='HCM_CL_DHQLY_007' and ma_vtcv='VNP-HNHCM_BHKV_52'
                 and ma_vtcv=a.ma_vtcv);
                 
update ct_bsc_cl_dhqly_007_bhol a
   set sl_dat=nvl((select sum(sl_dat) from ct_bsc_cl_dhqly_007_bhol where thang=a.thang and ma_to=a.ma_to and ma_vtcv<>a.ma_vtcv),0)
      ,sl_khongdat=(select sum(sl_khongdat) from ct_bsc_cl_dhqly_007_bhol where thang=a.thang and ma_to=a.ma_to and ma_vtcv<>a.ma_vtcv)
 where thang=202412 
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202412 and ma_kpi='HCM_CL_DHQLY_007' and ma_vtcv='VNP-HNHCM_BHKV_52'
                 and ma_vtcv=a.ma_vtcv);
update ct_bsc_cl_dhqly_007_bhol a
   set tyle_thuchien=round(nvl(sl_dat,0)/(nvl(sl_dat,0)+nvl(sl_khongdat,0))*100,2)
 where thang=202412 
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202412 and ma_kpi='HCM_CL_DHQLY_007' and ma_vtcv='VNP-HNHCM_BHKV_52'
                 and ma_vtcv=a.ma_vtcv)
   and (nvl(sl_dat,0)+nvl(sl_khongdat,0))>0;

update ct_bsc_cl_dhqly_007_bhol a
   set diem_cong=case when tyle_thuchien=100 and mdo_hthanh_dthu_ptm>=100 then 20 end 
      ,diem_tru=case when tyle_thuchien<50 then 20 end 
 where thang=202412 
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202412 and ma_kpi='HCM_CL_DHQLY_007' and ma_vtcv='VNP-HNHCM_BHKV_52'
                 and ma_vtcv=a.ma_vtcv);                                  
 
--select * from ct_bsc_cl_dhqly_007_bhol where thang=202412
 
  
update bangluong_kpi a
   set (diem_cong,diem_tru)=(select diem_cong,diem_tru 
                               from ct_bsc_cl_dhqly_007_bhol 
                              where thang=a.thang and ma_nv=a.ma_nv
                                and ma_nv=ma_nhanvien)
      -- update bangluong_kpi a set diem_cong='',diem_tru='' 
      --    select * from bangluong_kpi a                           
 where thang=202412 and ma_kpi='HCM_CL_DHQLY_007'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)=upper(a.ma_kpi) and ma_vtcv='VNP-HNHCM_BHKV_52' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv)
   and exists(select * from ct_bsc_cl_dhqly_007_bhol  where thang=a.thang and ma_nv=a.ma_nv and ma_nv=ma_nhanvien); 

update bangluong_kpi a set diem_cong=0                          
 where thang=202412 and ma_kpi='HCM_CL_DHQLY_007'
   and diem_cong is null;
   
update bangluong_kpi a set diem_tru=0                         
 where thang=202412 and ma_kpi='HCM_CL_DHQLY_007'
   and diem_tru is null;
   
----------------------------------------HCM_CL_DHQLY_006 -- TT
--create table ct_bsc_cl_dhqly_006_tt_202412_l1 as select * from ct_bsc_cl_dhqly_006_tt where thang=202412;

delete from ct_bsc_cl_dhqly_006_tt where thang=202412;
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
 where thang=202412 and ma_kpi='HCM_DT_PTMOI_021'
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
 where thang=202412 
   and not exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=a.thang and ma_kpi='HCM_CL_DHQLY_006' 
                 and (to_truong_pho is not null or giamdoc_phogiamdoc is not null)
                 and ma_vtcv=a.ma_vtcv);
                                 
update ct_bsc_cl_dhqly_006_tt a
   set sl_dat=(select nvl(sum(sl_dat),0) from ct_bsc_cl_dhqly_006_tt where thang=a.thang and ma_to=a.ma_to and ma_vtcv<>a.ma_vtcv)
      ,sl_khongdat=(select nvl(sum(sl_khongdat),0) from ct_bsc_cl_dhqly_006_tt where thang=a.thang and ma_to=a.ma_to and ma_vtcv<>a.ma_vtcv)
 where thang=202412 
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=a.thang and ma_kpi='HCM_CL_DHQLY_006' and to_truong_pho is not null
                 and ma_vtcv=a.ma_vtcv);                 
                 
update ct_bsc_cl_dhqly_006_tt a
   set tyle_thuchien=round(sl_dat/(nvl(sl_dat,0)+nvl(sl_khongdat,0))*100,2)
 where thang=202412 
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=a.thang and ma_kpi='HCM_CL_DHQLY_006' and to_truong_pho is not null
                 and ma_vtcv=a.ma_vtcv)
   and (nvl(sl_dat,0)+nvl(sl_khongdat,0))>0;                
                 
update ct_bsc_cl_dhqly_006_tt a
   set diem_cong=case when tyle_thuchien=100 and mucdo_hoanthanh>=100 then 20 end 
      ,diem_tru=case when tyle_thuchien<50 then 20 end 
 where thang=202412 
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202412 and ma_kpi='HCM_CL_DHQLY_006' and to_truong_pho is not null
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
 where thang=202412 and ma_kpi='HCM_CL_DHQLY_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)=upper(a.ma_kpi) and to_truong_pho is not null and ma_vtcv<>'VNP-HNHCM_BHKV_17'
                  and ma_vtcv=a.ma_vtcv);


update bangluong_kpi a set diem_cong=0                       
 where thang=202412 and ma_kpi='HCM_CL_DHQLY_006'
   and diem_cong is null; 

update bangluong_kpi a set diem_tru=0                    
 where thang=202412 and ma_kpi='HCM_CL_DHQLY_006'
   and diem_tru is null; 
   
----------------------------------------HCM_CL_DHQLY_006 -- PGD
--create table ct_bsc_cl_dhqly_006_pgd_202412_l1 as select * from ct_bsc_cl_dhqly_006_pgd where thang=202412;

delete from ct_bsc_cl_dhqly_006_pgd where thang=202412;
insert into ct_bsc_cl_dhqly_006_pgd
select 202412 thang,ma_pb,ma_nv,ma_nhanvien
      ,sum(nhombrcd_dtgiao)nhombrcd_dtgiao,sum(nhombrcd_kqth)nhombrcd_kqth
      ,sum(nhomvina_dtgiao)nhomvina_dtgiao,sum(nhomvina_kqth)nhomvina_kqth
      ,sum(nhomcntt_dtgiao)nhomcntt_dtgiao,sum(nhomcntt_kqth)nhomcntt_kqth
      ,sum(nhomtsl_dtgiao)nhomtsl_dtgiao,sum(nhomconlai_kqth)nhomconlai_kqth
      ,null tong_dthu_giao,null tong_dthu_thuchien,null tyle_thuchien,null mucdo_hoanthanh
      ,null sl_dat,null sl_khongdat,null tyle_dat,null diem_cong,null diem_tru,null ghichu    
  from(
  
select ma_pb
      ,(select distinct ma_nv from blkpi_dm_to_pgd 
         where thang=a.thang and ma_kpi='HCM_DT_PTMOI_021' and dichvu in ('Mega+Fiber','MyTV') and ma_to=a.ma_to)ma_nv
      ,ma_nv ma_nhanvien,nhombrcd_dtgiao,nhombrcd_kqth,null nhomvina_dtgiao,null nhomvina_kqth
      ,null nhomcntt_dtgiao,null nhomcntt_kqth,null nhomtsl_dtgiao,null nhomconlai_kqth     
  from dinhmuc_giao_dthu_ptm a
 where thang=202412 and nhombrcd_dtgiao>0
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=a.thang and upper(ma_kpi)='HCM_DT_PTMOI_021' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv)
   and exists(select * from nhanvien where thang=a.thang and tinh_bsc=1 and thaydoi_vtcv=0 and ma_nv=a.ma_nv)
                  
 union all
select ma_pb
      ,(select distinct ma_nv from blkpi_dm_to_pgd 
         where thang=a.thang and ma_kpi='HCM_DT_PTMOI_021' and dichvu in('VNP tra sau','VNP tra truoc') and ma_to=a.ma_to)ma_nv
      ,ma_nv ma_nhanvien,null nhombrcd_dtgiao,null nhombrcd_kqth,nhomvina_dtgiao,nvl(nhomvinats_kqth,0)+nvl(nhomvinatt_kqth,0)nhomvina_kqth
      ,null nhomcntt_dtgiao,null nhomcntt_kqth,null nhomtsl_dtgiao,null nhomconlai_kqth
  from dinhmuc_giao_dthu_ptm a
 where thang=202412 and nhomvina_dtgiao>0
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=a.thang and upper(ma_kpi)='HCM_DT_PTMOI_021' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv)
   and exists(select * from nhanvien where thang=a.thang and tinh_bsc=1 and thaydoi_vtcv=0 and ma_nv=a.ma_nv)
   
 union all
select ma_pb
      ,(select ma_nv from blkpi_dm_to_pgd 
         where thang=a.thang and ma_kpi='HCM_DT_PTMOI_021' and dichvu='Dich vu so doanh nghiep' and ma_to=a.ma_to)ma_nv
      ,ma_nv ma_nhanvien,null nhombrcd_dtgiao,null nhombrcd_kqth,null nhomvina_dtgiao,null nhomvina_kqth
      ,nhomcntt_dtgiao,nhomcntt_kqth,null nhomtsl_dtgiao,null nhomconlai_kqth 
  from dinhmuc_giao_dthu_ptm a
 where thang=202412 and nhomcntt_dtgiao>0
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=a.thang and upper(ma_kpi)='HCM_DT_PTMOI_021' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv)
   and exists(select * from nhanvien where thang=a.thang and tinh_bsc=1 and thaydoi_vtcv=0 and ma_nv=a.ma_nv)
                  
 union all
select ma_pb
      ,(select ma_nv from blkpi_dm_to_pgd 
         where thang=a.thang and ma_kpi='HCM_DT_PTMOI_021' and dichvu='Con lai' and ma_to=a.ma_to)ma_nv
      ,ma_nv ma_nhanvien,null nhombrcd_dtgiao,null nhombrcd_kqth,null nhomvina_dtgiao,null nhomvina_kqth
      ,null nhomcntt_dtgiao,null nhomcntt_kqth,nhomtsl_dtgiao,nhomconlai_kqth
  from dinhmuc_giao_dthu_ptm a
 where thang=202412 and nhomtsl_dtgiao>0
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=a.thang and upper(ma_kpi)='HCM_DT_PTMOI_021' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv)               
   and exists(select * from nhanvien where thang=a.thang and tinh_bsc=1 and thaydoi_vtcv=0 and ma_nv=a.ma_nv)
                  
)where ma_nv is not null
   and ma_nv not in ('VNP016659','VNP017585','VNP017947','VNP017729','VNP016898','VNP017496','VNP016983') ----- ko phu trach CSKH
 group by ma_pb,ma_nv,ma_nhanvien;


update ct_bsc_cl_dhqly_006_pgd
   set tong_dthu_giao=nvl(nhombrcd_dtgiao,0)+nvl(nhomvina_dtgiao,0)+nvl(nhomcntt_dtgiao,0)+nvl(nhomtsl_dtgiao,0)
      ,tong_dthu_thuchien=nvl(nhombrcd_kqth,0)+nvl(nhomvina_kqth,0)+nvl(nhomcntt_kqth,0)+nvl(nhomconlai_kqth,0)
 where thang=202412;

update ct_bsc_cl_dhqly_006_pgd
   set tyle_thuchien=round(tong_dthu_thuchien/tong_dthu_giao*100,2)
 where thang=202412 and tong_dthu_giao>0;
 
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
 where thang=202412; 

update ct_bsc_cl_dhqly_006_pgd
   set sl_dat=case when mucdo_hoanthanh>=100 then 1 end,sl_khongdat=case when mucdo_hoanthanh<100 then 1 end
 where thang=202412;


---- PGD phu trach CSKH
insert into ct_bsc_cl_dhqly_006_pgd(thang,ma_pb,ma_nv,ma_nhanvien,tong_dthu_giao,tong_dthu_thuchien
                      ,tyle_thuchien,mucdo_hoanthanh,sl_dat,sl_khongdat,ghichu)
with 
danhmuc_pgd as(
select distinct ma_pb,ma_to,ma_nv 
  from blkpi_dm_to_pgd 
 where thang=202412 and ma_kpi='HCM_DT_PTMOI_021'
   and ma_nv in ('VNP016659','VNP017585','VNP017947','VNP017729','VNP016898','VNP017496','VNP016983'))
                 
select 202412,ma_pb--,ten_pb
      ,(select ma_nv from danhmuc_pgd where ma_to=a.ma_to)ma_nv --PGD
      ,ma_nv ma_nhanvien,giao,thuchien,tyle_thuchien,mucdo_hoanthanh
      ,case when mucdo_hoanthanh>=100 then 1 end dat,case when nvl(mucdo_hoanthanh,0)<100 then 1 end khongdat
      ,'tinh cho pgd cskh'ghichu  
  from bangluong_kpi a
 where a.thang=202412 and a.ma_kpi='HCM_DT_PTMOI_021' and a.ma_vtcv not in ('VNP-HNHCM_BHKV_28','VNP-HNHCM_BHKV_52')
   and (select ma_nv from danhmuc_pgd where ma_to=a.ma_to) is not null
   and exists(select * from nhanvien where thang=a.thang and tinh_bsc=1 and thaydoi_vtcv=0 and ma_nv=a.ma_nv); 

insert into ct_bsc_cl_dhqly_006_pgd(thang,ma_pb,ma_nv,ma_nhanvien,mucdo_hoanthanh)
select thang,ma_pb,ma_nv,' TONG CONG'ma_nhanvien
      ,(select mucdo_hoanthanh from bangluong_kpi where thang=a.thang and ma_kpi='HCM_DT_PTMOI_021' and ma_nv=a.ma_nv)
  from nhanvien a
 where thang=202412
   and exists(select * from ct_bsc_cl_dhqly_006_pgd where thang=a.thang and ma_nv=a.ma_nv)
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=a.thang and ma_kpi='HCM_CL_DHQLY_006' and giamdoc_phogiamdoc is not null
                 and ma_vtcv=a.ma_vtcv)
   and exists(select * from nhanvien where thang=a.thang and tinh_bsc=1 and thaydoi_vtcv=0 and ma_nv=a.ma_nv);



update ct_bsc_cl_dhqly_006_pgd a
   set sl_dat=(select nvl(sum(sl_dat),0) from ct_bsc_cl_dhqly_006_pgd where thang=a.thang and ma_nhanvien<>' TONG CONG' and ma_nv=a.ma_nv)
      ,sl_khongdat=(select nvl(sum(sl_khongdat),0) from ct_bsc_cl_dhqly_006_pgd where thang=a.thang and ma_nhanvien<>' TONG CONG' and ma_nv=a.ma_nv)
 where thang=202412 and ma_nhanvien=' TONG CONG';   
 
update ct_bsc_cl_dhqly_006_pgd a
   set tyle_dat=round(nvl(sl_dat,0)/(nvl(sl_dat,0)+nvl(sl_khongdat,0))*100,2)
 where thang=202412 and ma_nhanvien=' TONG CONG';  

update ct_bsc_cl_dhqly_006_pgd a
   set diem_cong=case when tyle_dat=100 and mucdo_hoanthanh>=100 then 20 end 
      ,diem_tru=case when tyle_dat<50 then 20 end
 where thang=202412 and ma_nhanvien=' TONG CONG'; 
 


update bangluong_kpi a
   set (diem_cong,diem_tru)=(select diem_cong,diem_tru 
                               from ct_bsc_cl_dhqly_006_pgd
                              where thang=a.thang and ma_nhanvien=' TONG CONG' and ma_nv=a.ma_nv)
      -- update bangluong_kpi set tyle_thuchien=''
      --    select * from bangluong_kpi a                           
 where thang=202412 and ma_kpi='HCM_CL_DHQLY_006'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_CL_DHQLY_006' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv);

update bangluong_kpi a set diem_cong=0                         
 where thang=202412 and ma_kpi='HCM_CL_DHQLY_006'
   and diem_cong is null;

update bangluong_kpi a set diem_tru=0                         
 where thang=202412 and ma_kpi='HCM_CL_DHQLY_006'
   and diem_tru is null;
   
   
   
   
----------------------------------------HCM_SL_HOTRO_001
alter table ct_nghiepvu_sbh drop index ct_nghiepvu_sbh_matb;
delete from ct_nghiepvu_sbh where thang=202412;
insert into ct_nghiepvu_sbh
select distinct 202412 thang
      ,case when loai like '%ONEBSS%' then ma_gd end ma_gd
      ,ma_tb,ma_kh,loai,ten_loaihd,ngay_cn
      ,null nhom_tinh--(select * from dm_nghiepvu_sbh where loai=a.loai and ten_loaihd=a.ten_loaihd)nhom_tinh
      ,case when loai like '%ONEBSS%' and ma_gd is null then 'Khong co ma giao dich' end khong_tinh
      ,manv_ra_pct ma_nv
      ,(select ma_to from nhanvien where thang=202412 and ma_nv=a.manv_ra_pct and rownum=1)ma_to
      ,(select ma_pb from nhanvien where thang=202412 and ma_nv=a.manv_ra_pct and rownum=1)ma_pb
      ,''ghichu
     -- select count(*)
  from tuyenngo.SBH_GIAODICH_202412_ct_new a 
 --where exists(select * from blkpi_danhmuc_kpi_vtcv where thang=202412 and ma_kpi='HCM_SL_HOTRO_001' and ma_vtcv=a.ma_vtcv)
;
--create table ct_nghiepvu_sbh_202412_15012025 as
select *from ct_nghiepvu_sbh where thang=202412   and ma_tb='84946997695';


insert into ct_nghiepvu_sbh(thang,ma_tb,loai,ten_loaihd,ngay_cn,ma_nv,ghichu)
select 202412 thang,ma_tb,'VNP - CCBS'loai,ten_loaihd,ngay_cn_date,ma_nv,'file bs'ghichu
  from temp_nv a
 where not exists(select *
                   from tuyenngo.SBH_GIAODICH_202412_ct_new 
                  where ten_loaihd in ('DONG MO DV|1','DONG MO DV|0') and ma_tb=a.ma_tb);

update ct_nghiepvu_sbh a
   set (ma_to,ma_pb)=(select ma_to,ma_pb from nhanvien where thang=202412 and ma_nv=a.ma_nv)
 where thang=202412 and ma_pb is null;
 

create index ct_nghiepvu_sbh_matb on ct_nghiepvu_sbh(ma_tb);
                  
/*--insert into dm_nghiepvu_sbh
select distinct loai,ten_loaihd,''
  from ct_nghiepvu_sbh a
 where thang=202412 and not exists(select * from dm_nghiepvu_sbh where loai=a.loai and ten_loaihd=a.ten_loaihd)
*/


------------ loc trung xet trong ngay hay trong thang
update ct_nghiepvu_sbh a set khong_tinh='Trung thao tac DOI SIM ben ccbs'
 where thang=202412
   and loai='USSD' and ten_loaihd='DOI SIM'
   and exists(select * from ct_nghiepvu_sbh where thang=202412 and loai='VNP - CCBS' and ten_loaihd='DOI SIM' and ma_tb=a.ma_tb);


update ct_nghiepvu_sbh a set khong_tinh='Trung thao tac khac cua cung thue bao trong ky'
 where thang=202412
   and ten_loaihd='CAP NHAT DB'
   and exists(select * from ct_nghiepvu_sbh 
               where thang=202412 and khong_tinh is null and ten_loaihd<>'CAP NHAT DB' 
                 and ma_tb=a.ma_tb);


update ct_nghiepvu_sbh a set khong_tinh='Trung thao tac khac cua cung thue bao trong ky'
 --select * from ct_nghiepvu_sbh a
 where thang=202412
   and ten_loaihd in ('DONG MO DV|1','DONG MO DV|0')
   and exists(select * from ct_nghiepvu_sbh 
               where thang=202412 and khong_tinh is null and ten_loaihd not in ('DONG MO DV|1','DONG MO DV|0') 
                 and ma_tb=a.ma_tb);
   


drop table temp_sbh;
create table temp_sbh as

select loai,ten_loaihd,ma_nv,1 quydoi
  from ct_nghiepvu_sbh 
 where thang=202412 and khong_tinh is null 
   and ten_loaihd not in ('DONG MO DV|0','DONG MO DV|1','BIEN DONG KHAC','THU KHAC')
 
  union all
select loai,ten_loaihd,ma_nv,floor(count(*)/100)+case when mod(count(*),100)>0 then 1 else 0 end quydoi 
  from(select distinct loai,'DONG MO DV|0|1'ten_loaihd,ma_nv,ma_kh,ma_tb,to_char(ngay_cn,'dd/mm/yyyy')ngay_cn
         from ct_nghiepvu_sbh 
        where thang=202412 and khong_tinh is null and ten_loaihd in ('DONG MO DV|0','DONG MO DV|1'))
 group by loai,ten_loaihd,ma_nv,ma_kh,ngay_cn
 
  union all
select loai,ten_loaihd,ma_nv,count(distinct ma_tb)quydoi
  from ct_nghiepvu_sbh 
 where thang=202412 and khong_tinh is null and ten_loaihd='BIEN DONG KHAC'
 group by loai,ten_loaihd,ma_nv

 union all
select 'THU CUOC'loai,'THU CUOC'ten_loaihd,ma_nv,count(distinct ma_kh)*0.25
  from ct_nghiepvu_sbh a
 where thang=202412 and khong_tinh is null and ten_loaihd='THU KHAC'
 group by ma_to,ma_nv
 ;


select a.*,(select ten_nv from nhanvien where thang=202412 and ma_nv=a.ma_nv)ten_nv
  from ct_nghiepvu_sbh a
 where thang=202412 
   and ma_pb in('VNP0702300','VNP0702400','VNP0702500')
 union all  
select a.*,(select ten_nv from nhanvien where thang=202412 and ma_nv=a.ma_nv)ten_nv
  from ct_nghiepvu_sbh a
 where thang=202412 
   and (ma_pb not in('VNP0702300','VNP0702400','VNP0702500') or ma_pb is null)
   and exists(select * from ct_nghiepvu_sbh 
               where thang=202412 and ma_pb in('VNP0702300','VNP0702400','VNP0702500')
                 and ma_tb=a.ma_tb)


select ten_pb,ten_to,ma_nv,ten_nv,sum(quydoi)quydoi from(
select (select ten_pb from nhanvien where thang=202412 and ma_nv=a.ma_nv)ten_pb
      ,(select ten_to from nhanvien where thang=202412 and ma_nv=a.ma_nv)ten_to
      ,ma_nv,(select ten_nv from nhanvien where thang=202412 and ma_nv=a.ma_nv)ten_nv
      ,loai,ten_loaihd,sum(quydoi)quydoi
     /* ,(select count(*) from ct_nghiepvu_sbh
         where thang=202412 and khong_tinh is null 
           and case when ten_loaihd in ('DONG MO DV|0','DONG MO DV|1') then 'DONG MO DV|0|1' else ten_loaihd end=a.ten_loaihd
           and ma_nv=a.ma_nv)sl_thuchien_goc*/
           --select *
  from temp_sbh a  
 where ma_nv in (select ma_nv from nhanvien 
                  where thang=202412
                    and ma_vtcv in (select ma_vtcv from blkpi_danhmuc_kpi_vtcv where thang=202412 and ma_kpi='HCM_SL_HOTRO_001')) 
 group by ma_nv,loai,ten_loaihd
  )group by ten_pb,ten_to,ma_nv,ten_nv order by quydoi desc;


select * from dm_nghiepvu_sbh


update bangluong_kpi a
   set thuchien=(select sum(quydoi) from temp_sbh where ma_nv=a.ma_nv) 
       --update bangluong_kpi a set thuchien=''
       --select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_SL_HOTRO_001'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_SL_HOTRO_001' and to_truong_pho is null
                  and ma_vtcv=a.ma_vtcv);


update bangluong_kpi a
   set tyle_thuchien=round(thuchien/giao*100,2)
      ,mucdo_hoanthanh=least(round(thuchien/giao*100,2),120)
       --   update bangluong_kpi a set tyle_thuchien='',mucdo_hoanthanh=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_SL_HOTRO_001';

 
----------------------------------------HCM_SL_CSKHH_003
------------------ so giao c?a PGD chua co

drop table SBH_GIAODICH_202412_CT_new;
create table SBH_GIAODICH_202412_CT_new as 
select ma_gd,ma_tb,ma_kh,ten_loaihd,loai,nhomtinh,ngay_cn,rownum stt,cast(null as varchar(100))khong_tinh
      ,manv_ra_pct ma_nv
      ,(select ma_to from nhanvien where thang=202412 and ma_nv=b.manv_ra_pct)ma_to
      ,(select ma_pb from nhanvien where thang=202412 and ma_nv=b.manv_ra_pct)ma_pb 
  from(select distinct ma_gd,ma_tb,ma_kh,manv_ra_pct,ten_loaihd,loai,ngay_cn
             ,(select nhomtinh from dm_haumai_gdv where loai=a.loai and ten_loaihd=a.ten_loaihd)nhomtinh
         from tuyenngo.SBH_GIAODICH_202412_CT_new a
        where manv_ra_pct is not null
          and exists (select * from blkpi_danhmuc_kpi_vtcv 
                       where thang=202412 and upper(ma_kpi)='HCM_SL_CSKHH_003' and ma_vtcv=a.ma_vtcv)
        order by ten_loaihd,loai,ngay_cn)b;

update SBH_GIAODICH_202412_CT_new set khong_tinh='Khong co thong tin: ma giao dich, ma KH, ma thue bao'
 where ma_gd is null and ma_kh is null and ma_tb is null;

update SBH_GIAODICH_202412_CT_new set khong_tinh='thao tac tu ID163,164'
 where (ma_tb,ma_nv) in (select case when length(b.matb)=9 and lpad(b.matb,2) in ('81','82','83','84','85','88','91','94')
                                         then '84'||b.matb
                                     else b.matb end 
                               ,a.ma_nv
                           from ttkd_bsc.ktdt_ct_bsc_xl_nghievu_cskh a,ttkdhcm_ktnv.yckh_thuebao b
                          where a.thang=202412 and a.id_yckh=b.id_yckh);



drop table temp_giahan_tratruoc;
create table temp_giahan_tratruoc as
select distinct b.ma_tb,a.ma_gd
  from css.v_hd_khachhang@dataguard a
       join css.v_hd_thuebao@dataguard b on a.hdkh_id=b.hdkh_id
       join css.v_ct_tienhd@dataguard c on b.hdtb_id=c.hdtb_id
 where kieuld_id in (551,550,24,13080) and c.khoanmuctt_id=11 and c.tien>0 and to_char(ngay_yc,'yyyymm')='202412';
create index temp_giahan_tratruoc_1 on temp_giahan_tratruoc(ma_tb,ma_gd);

update SBH_GIAODICH_202412_CT_new a set khong_tinh='Trong tien trinh GHTT'
 where exists(select * from temp_giahan_tratruoc where ma_tb=a.ma_tb and ma_gd=a.ma_gd);
   
 

update SBH_GIAODICH_202412_CT_new a set khong_tinh='Trong tien trinh lap dat moi'
 where exists(select * from SBH_GIAODICH_202412_CT_new 
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
  and to_char(ngay_yc,'yyyymm')='202412';
  
  
update SBH_GIAODICH_202412_CT_new a set khong_tinh='KH co yc thao tac khac'
 where loai='VNP - CCBS' and ten_loaihd='THU CUOC'
   and exists(select * from SBH_GIAODICH_202412_CT_new where ten_loaihd not in ('THU CUOC','CAP NHAT DB') and ma_kh=a.ma_kh);
 
 
update SBH_GIAODICH_202412_CT_new a set khong_tinh='KH co yc thao tac khac'
 where loai='THU CUOC ONEBSS' and ten_loaihd='THU CUOC'
   and exists(select * from SBH_GIAODICH_202412_CT_new where ten_loaihd!='THU CUOC' and ma_kh=a.ma_kh);

update SBH_GIAODICH_202412_CT_new a set khong_tinh='KH da duoc nhan tin'
 where loai='THU CUOC ONEBSS' and ten_loaihd='THU CUOC'
   and exists(select * from ttkdhcm_ktnv.gdv_haumai where to_char(ngay_yc,'yyyymm')='202412' and no_sms=1 and ma_tb=a.ma_tb);

update SBH_GIAODICH_202412_CT_new a set khong_tinh='KH da duoc nhan tin (dai)'
 where loai='THU CUOC ONEBSS' and ten_loaihd='THU CUOC'
   and exists(select * from loai_sms_dai where ma_tb=a.ma_tb);
   
   
update SBH_GIAODICH_202412_CT_new a set khong_tinh='KH co yc thao tac khac'
 where loai='ONEBSS' and ten_loaihd='THU KHAC'
   and exists(select * from SBH_GIAODICH_202412_CT_new where ten_loaihd!='THU KHAC' and ma_kh=a.ma_kh);

update SBH_GIAODICH_202412_CT_new a set khong_tinh='KH da duoc nhan tin'
 where loai='ONEBSS' and ten_loaihd='THU KHAC'
   and exists(select * from ttkdhcm_ktnv.gdv_haumai where to_char(ngay_yc,'yyyymm')='202412' and no_sms=1 and ma_tb=a.ma_tb);

update SBH_GIAODICH_202412_CT_new a set khong_tinh='KH da duoc nhan tin (dai)'
 where loai='ONEBSS' and ten_loaihd='THU KHAC'
   and exists(select * from loai_sms_dai where ma_tb=a.ma_tb);


update SBH_GIAODICH_202412_CT_new a set khong_tinh='Trung khi dong bo doi sim ve ccbs'
  --select * from SBH_GIAODICH_202412_CT_new a
 where loai='USSD' and ten_loaihd='DOI SIM'
   and exists(select * from SBH_GIAODICH_202412_CT_new where loai='VNP - CCBS' and ten_loaihd='DOI SIM' and ma_tb=a.ma_tb);



update SBH_GIAODICH_202412_CT_new a set khong_tinh='Cap nhat DB da dong bo ben Nghiep vu khac'
 where loai='VNP - CCBS' and ten_loaihd='CAP NHAT DB'
   and exists(select * from SBH_GIAODICH_202412_CT_new 
               where loai='VNP - CCBS' and ten_loaihd<>'CAP NHAT DB' 
                 and ma_tb=a.ma_tb);-- and ma_nv=a.ma_nv);

commit;


------- Chuyen doi VCC
alter table SBH_GIAODICH_202412_CT_new add chuyen_vcc varchar(100);

update SBH_GIAODICH_202412_CT_new a 
   set chuyen_vcc=1
        --select * from SBH_GIAODICH_202412_CT_new a
 where exists(select * from ttkd_bsc.dt_ptm_vnp_202412_bs where somay=a.ma_tb)
   and ten_loaihd='DANG KY HUY CHUYEN DOI GOI CUOC';
                    
                    
select * from SBH_GIAODICH_202412_CT_new 
 where ten_loaihd='DANG KY HUY CHUYEN DOI GOI CUOC' 
   and chuyen_vcc is not null;


select distinct tenkieu_ld from ttkd_bsc.ct_bsc_ptm 
                    where thang_ptm=202412 and loaitb_id=149


alter table SBH_GIAODICH_202412_CT_new add ten_tb varchar(400);
alter table SBH_GIAODICH_202412_CT_new add ma_dt_kh varchar(2);

update SBH_GIAODICH_202412_CT_new a
   set (ten_tb,ma_dt_kh)=(select ten_tb,ma_dt_kh from ttkd_bct.db_thuebao_ttkd where ma_tb=a.ma_tb and ma_tt=a.ma_kh);
   


drop table tmp_ct_gdv;
create table tmp_ct_gdv as

select ma_to,ma_nv,count(distinct ma_kh)*0.25 sl,loai,ten_loaihd
  from SBH_GIAODICH_202412_CT_new a
 where loai='VNP - CCBS' and ten_loaihd='THU CUOC'
   and khong_tinh is null
 group by ma_to,ma_nv,loai,ten_loaihd
   
 union all

select ma_to,ma_nv,count(distinct ma_kh)*0.25 sl,loai,ten_loaihd
  from SBH_GIAODICH_202412_CT_new a
 where loai='THU CUOC ONEBSS' and ten_loaihd='THU CUOC'
   and khong_tinh is null
 group by ma_to,ma_nv,loai,ten_loaihd
 
  union all

select ma_to,ma_nv,count(distinct ma_kh)*0.25 sl,loai,ten_loaihd
  from SBH_GIAODICH_202412_CT_new a
 where loai='ONEBSS' and ten_loaihd='THU KHAC'
   and khong_tinh is null
 group by ma_to,ma_nv,loai,ten_loaihd
                   
 union all
select ma_to,ma_nv,count(*)sl,loai,ten_loaihd
  from SBH_GIAODICH_202412_CT_new a
 where nhomtinh=1 
   and khong_tinh is null
 group by ma_to,ma_nv,loai,ten_loaihd
         
 union all
select ma_to,ma_nv,count(*)sl,loai,ten_loaihd
  from (select * from SBH_GIAODICH_202412_CT_new where khong_tinh is null)a
 where nhomtinh=2 -- nhom 2: lay thao tac cuoi cung trong ngay
   and stt=(select max(stt) from SBH_GIAODICH_202412_CT_new 
             where nhomtinh=2 
               and loai=a.loai and ten_loaihd=a.ten_loaihd and ma_tb=a.ma_tb 
               and to_char(ngay_cn,'yyyymmdd')=to_char(a.ngay_cn,'yyyymmdd')
           )       
 group by ma_to,ma_nv,loai,ten_loaihd   
 
  union all

select ma_to,ma_nv,count(*)sl,loai,ten_loaihd
  from (select * from SBH_GIAODICH_202412_CT_new where khong_tinh is null)a
 where nhomtinh=3 -- nhom 3: lay thao tac cuoi cung trong thang
   and stt=(select max(stt) from SBH_GIAODICH_202412_CT_new 
             where nhomtinh=3 
               and loai=a.loai and ten_loaihd=a.ten_loaihd and ma_tb=a.ma_tb 
               and to_char(ngay_cn,'yyyymm')=to_char(a.ngay_cn,'yyyymm')
           )
 group by ma_to,ma_nv,loai,ten_loaihd 
;    
 


update bangluong_kpi a
   set thuchien=(select nvl(sum(sl)*1.1,0) from tmp_ct_gdv where ma_nv=a.ma_nv)
       --   update bangluong_kpi set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_SL_CSKHH_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_SL_CSKHH_003' and to_truong_pho is null and giamdoc_phogiamdoc is null
                  and ma_vtcv=a.ma_vtcv);
 
 
 
update bangluong_kpi a
   set thuchien=(select sum(sl)*1.1 from tmp_ct_gdv where ma_to=a.ma_to)
       --   update bangluong_kpi a set thuchien=''
       --   select sum(giao),count(*) from bangluong_kpi a
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_SL_CSKHH_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_SL_CSKHH_003' and to_truong_pho is not null
                  and ma_vtcv=a.ma_vtcv);
                                    

-- bo sung PGD Binh Chanh
-- select distinct ma_to,ten_to from ttkd_bsc.nhanvien where thang=202412 and ma_pb='VNP0701100'

update bangluong_kpi a
   set thuchien=(select sum(sl)*1.1 from tmp_ct_gdv where ma_to in ('VNP0701180','VNP0701190','VNP07011A0'))
       --   update bangluong_kpi a set thuchien=''
       --   select sum(giao),count(*) from bangluong_kpi a
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_SL_CSKHH_003'
   and exists (select * from blkpi_danhmuc_kpi_vtcv 
                where thang=202412 and upper(ma_kpi)='HCM_SL_CSKHH_003' and giamdoc_phogiamdoc is not null
                  and ma_vtcv=a.ma_vtcv)
   and ma_nv='VNP016983';
                                    
-- select * from bangluong_kpi where ma_kpi='HCM_SL_CSKHH_003' and ma_nv='VNP016983';   
   
   
                 
                  
update bangluong_kpi a
   set tyle_thuchien=round(thuchien/giao*100,2)
      ,mucdo_hoanthanh=least(round(thuchien/giao*100,2),120)
       --   update bangluong_kpi a set thuchien=''
       --   select * from bangluong_kpi a
 where thang=202412 and ma_kpi='HCM_SL_CSKHH_003'





/*

drop table temp_sbh;
create table temp_sbh as

select loai,ten_loaihd,ma_nv,1 quydoi
  from ct_nghiepvu_sbh 
 where thang=202412 and khong_tinh is null and nhom_tinh=1
 
  union all
select loai,ten_loaihd,ma_nv,floor(count(*)/100)+case when mod(count(*),100)>0 then 1 else 0 end quydoi 
  from(select distinct loai,'DONG MO DV|0|1'ten_loaihd,ma_nv,ma_kh,ma_tb,to_char(ngay_cn,'dd/mm/yyyy')ngay_cn
         from ct_nghiepvu_sbh 
        where thang=202412 and khong_tinh is null and nhom_tinh=2)
 group by loai,ten_loaihd,ma_nv,ma_kh,ngay_cn
 
  union all
select loai,ten_loaihd,ma_nv,floor(count(*)/100)+case when mod(count(*),100)>0 then 1 else 0 end quydoi
  from(select distinct loai,ten_loaihd,ma_nv,ma_kh,ma_tb,to_char(ngay_cn,'dd/mm/yyyy')ngay_cn
         from ct_nghiepvu_sbh 
        where thang=202412 and khong_tinh is null and nhom_tinh=3)
 group by loai,ten_loaihd,ma_nv,ma_kh,ngay_cn

 union all
select 'THU CUOC'loai,'THU CUOC'ten_loaihd,ma_nv,count(distinct ma_kh)*0.25
  from ct_nghiepvu_sbh a
 where thang=202412 and khong_tinh is null and nhom_tinh=4
 group by ma_to,ma_nv
 ;
*/

