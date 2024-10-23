
delete from ct_bsc_ptmoi_056 where thang=202409;
insert into ct_bsc_ptmoi_056
       (thang,ma_pb,ma_to,ma_nv,ma_vtcv
       ,cntt_dthu_giao,cntt_dthu_thuchien,cdbr_mytv_dthu_giao,cdbr_mytv_dthu_thuchien,didong_dthu_giao,didong_dthu_thuchien)   
select thang,ma_pb,ma_to,ma_nv,ma_vtcv
      ,nhomcntt_dtgiao,nhomcntt_kqth,nhombrcd_dtgiao,nhombrcd_kqth
      ,nhomvina_dtgiao,nvl(nhomvinats_kqth,0)+nvl(nhomvinatt_kqth,0)nhomvina_kqth
  from dinhmuc_giao_dthu_ptm a
 where thang=202409
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202409 and ma_kpi='HCM_DT_PTMOI_056' and to_truong_pho is null and giamdoc_phogiamdoc is null 
                 and ma_vtcv=a.ma_vtcv)
;


update ct_bsc_ptmoi_056 a
   set cntt_tyle_thuchien=case when cntt_dthu_giao>0 then round(nvl(cntt_dthu_thuchien,0)/cntt_dthu_giao*100,2) end
      ,cntt_diem_tru_bsc=case when cntt_dthu_giao>0 and nvl(cntt_dthu_thuchien,0)<cntt_dthu_giao*0.9 then 5 end
      ,cdbr_mytv_tyle_thuchien=case when cdbr_mytv_dthu_giao>0 then round(nvl(cdbr_mytv_dthu_thuchien,0)/cdbr_mytv_dthu_giao*100,2) end
      ,cdbr_mytv_diem_tru_bsc=case when cdbr_mytv_dthu_giao>0 and nvl(cdbr_mytv_dthu_thuchien,0)<cdbr_mytv_dthu_giao*0.9 then 5 end
      ,didong_tyle_thuchien=case when didong_dthu_giao>0 then round(nvl(didong_dthu_thuchien,0)/didong_dthu_giao*100,2) end
      ,didong_diem_tru_bsc=case when didong_dthu_giao>0 and nvl(didong_dthu_thuchien,0)<didong_dthu_giao*0.9 then 5 end   
 where thang=202409;

update ct_bsc_ptmoi_056 a
   set tong_diem_tru_bsc=nvl(cntt_diem_tru_bsc,0)+nvl(cdbr_mytv_diem_tru_bsc,0)+nvl(didong_diem_tru_bsc,0)
 where thang=202409;   
update ct_bsc_ptmoi_056 set tong_diem_tru_bsc='' where thang=202409 and tong_diem_tru_bsc=0;



update bangluong_kpi a set diem_tru=''
 where thang=202409 and ma_kpi='HCM_DT_PTMOI_056'
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202409 and ma_kpi='HCM_DT_PTMOI_056' and to_truong_pho is null and giamdoc_phogiamdoc is null
                 and ma_vtcv=a.ma_vtcv);
                 
update bangluong_kpi a
   set diem_tru=(select tong_diem_tru_bsc from ct_bsc_ptmoi_056 where thang=a.thang and ma_nv=a.ma_nv)
 where thang=202409 and ma_kpi='HCM_DT_PTMOI_056'
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202409 and ma_kpi='HCM_DT_PTMOI_056' and to_truong_pho is null and giamdoc_phogiamdoc is null
                 and ma_vtcv=a.ma_vtcv);
                      
commit; 


select * from bangluong_kpi a
 where thang=202409 and ma_kpi='HCM_DT_PTMOI_056'
   and exists(select * from blkpi_danhmuc_kpi_vtcv 
               where thang=202409 and ma_kpi='HCM_DT_PTMOI_056' and to_truong_pho is null and giamdoc_phogiamdoc is null
                 and ma_vtcv=a.ma_vtcv);