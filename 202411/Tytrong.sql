
----------------------------- HCM_SL_CSKHH_003
update bangluong_kpi a
   set tytrong=case when ma_vtcv='VNP-HNHCM_BHKV_22' then 20
                    when ma_vtcv in('VNP-HNHCM_BHKV_28','VNP-HNHCM_BHKV_27') then 20
                    when ma_vtcv='VNP-HNHCM_BHKV_2' then 20
                end
       --   update bangluong_kpi a set tytrong=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_SL_CSKHH_003';
 
 
 
----------------------------- HCM_SL_HOTRO_001
update bangluong_kpi set tytrong=100
       --update bangluong_kpi a set tytrong=''
       --select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_SL_HOTRO_001'; 
 
 
 
----------------------------- HCM_DT_LUYKE_002
update bangluong_kpi a
   set tytrong=case when ma_vtcv='VNP-HNHCM_KHDN_3' then 45
                    when ma_vtcv='VNP-HNHCM_KHDN_3.1' then 70
                    when ma_vtcv='VNP-HNHCM_KHDN_4' then 40
                    when ma_vtcv='VNP-HNHCM_KHDN_2' then 
                         case when ma_nv in ('VNP022074','VNP017621','VNP017699') then 30  --CSKH
                              else 40
                          end
                end
       --   update bangluong_kpi a set tytrong=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_LUYKE_002';
 
 
 
----------------------------- HCM_DT_LUYKE_003
update bangluong_kpi a
   set tytrong=case when ma_vtcv='VNP-HNHCM_KHDN_3' then 15
                    when ma_vtcv='VNP-HNHCM_KHDN_4' and ma_to<>'VNP0702309' then 15
                    when ma_vtcv='VNP-HNHCM_KHDN_2' then 
                         case when ma_nv in ('VNP022074','VNP017621','VNP017699') then 10  --CSKH
                              else 15
                          end
                end
       --   update bangluong_kpi a set tytrong=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_LUYKE_003';
 
 

----------------------------- HCM_DT_PTNAM_006
update bangluong_kpi a
   set tytrong=case when ma_vtcv='VNP-HNHCM_KHDN_3' then 20
                    when ma_vtcv='VNP-HNHCM_KHDN_4' and ma_to<>'VNP0702309' then 25
                    when ma_vtcv='VNP-HNHCM_KHDN_2' then 
                         case when ma_nv in ('VNP022074','VNP017621','VNP017699') then 10  --CSKH
                              else 25
                          end
                end
       --   update bangluong_kpi a set tytrong=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_PTNAM_006';
 
 
 
-------------------------HCM_DT_PTMOI_054
update bangluong_kpi a
   set tytrong=case when ma_vtcv='VNP-HNHCM_KHDN_3' then 10
                    when ma_vtcv='VNP-HNHCM_KHDN_4' then case when ma_to='VNP0702309' then 10 else 15 end
                    when ma_vtcv='VNP-HNHCM_KHDN_2' then 
                         case when ma_nv in ('VNP022074','VNP017621','VNP017699') then 10  --CSKH
                              else 15
                          end
                end
       --   update bangluong_kpi a set tytrong=''
       --   select * from bangluong_kpi a
 where thang=202411 and ma_kpi='HCM_DT_PTMOI_054';
 


--------------------- HCM_SL_DAILY_003
update bangluong_kpi a 
   set tytrong=case when ma_vtcv='VNP-HNHCM_KHDN_3.1' then 10
                    when ma_vtcv='VNP-HNHCM_KHDN_4' and ma_to='VNP0702309' then 20
                 end   
       --select * from bangluong_kpi a
       --update bangluong_kpi a set giao=''
 where thang=202411 and ma_kpi='HCM_SL_DAILY_003';



