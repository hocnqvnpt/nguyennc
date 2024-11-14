drop table ttkd_bct.v_db purge;
create table ttkd_bct.v_db as
			with tbl as(
						select thuebao_id, chuquan_id from css.v_db_adsl@dataguard where chuquan_id in (145, 264, 266)
						union all
						select thuebao_id, chuquan_id from css.v_db_gp@dataguard where chuquan_id in (145, 264, 266)
						union all
						select thuebao_id, chuquan_id from css.v_db_cd@dataguard where chuquan_id in (145, 264, 266)
						union all
						select thuebao_id, chuquan_id from css.v_db_ims@dataguard where chuquan_id in (145, 264, 266)
						union all
						select thuebao_id, chuquan_id from css.v_db_mgwan@dataguard where chuquan_id in (145, 264, 266)
						union all
						select thuebao_id, chuquan_id from css.v_db_tsl@dataguard where chuquan_id in (145, 264, 266) and DAUCUOI_ID = 1
						union all
						select thuebao_id, chuquan_id from css.v_db_cntt@dataguard where chuquan_id in (145, 264, 266)
					)
	select a.KHACHHANG_ID, kh.ma_kh, a.THANHTOAN_ID, a.THUEBAO_ID, MA_TB, NGAY_SD, NGAY_TD, NGAY_CAT, DICHVUVT_ID, LOAITB_ID, TRANGTHAITB_ID, tbl.CHUQUAN_ID
	from css.v_db_thuebao@dataguard a
				join css.v_db_khachhang@dataguard kh on a.khachhang_id = kh.khachhang_id
				join css.v_db_thanhtoan@dataguard tt on a.thanhtoan_id = tt.thanhtoan_id
				join tbl on a.thuebao_id = tbl.thuebao_id
;
create index ttkd_bct.idx_tbid_vdb on ttkd_bct.v_db (thuebao_id);

truncate table ttkd_bct.dbtb_ttkd_tmp;
--create table ttkd_bct.dbtb_ttkd_tmp as
----Insert hang ngay----DS PTM trong thang n (ngoai tru VNPts)
insert into ttkd_bct.dbtb_ttkd_tmp
select cast(null as number(15)) tb_id, db.KHACHHANG_ID, kh.MA_KH, db.THANHTOAN_ID, db.THUEBAO_ID, db.MA_TB, tt.MA_TT
			, db.NGAY_SD, db.NGAY_TD, db.NGAY_CAT, db.DICHVUVT_ID, db.LOAITB_ID, db.TRANGTHAITB_ID, db.CHUQUAN_ID
			, db.CHUQUAN_ID CHUQUAN_ID_OLD
			, 'onebss_moi' nguon, nvl2(db.thuebao_id, 1, 0) tontai_db
from  ttkd_bct.v_db db
			left join css_hcm.db_khachhang kh on db.khachhang_id = kh.khachhang_id
			left join css_hcm.db_thanhtoan tt on db.thanhtoan_id = tt.thanhtoan_id
where trunc(db.ngay_sd) between '01/10/2024' and '31/10/2024'
			and db.dichvuvt_id not in (2)
;
----Insert hang ngay----DS PTM trong thang n (VNPts)
insert into ttkd_bct.dbtb_ttkd_tmp
		select null tb_id, db.KHACHHANG_ID, kh.MA_KH, db.THANHTOAN_ID, db.THUEBAO_ID, a.SOMAY, a.ma_kh MA_TT
					, a.NGAY_LD, null, null,  a.DICHVUVT_ID, a.LOAITB_ID, a.TRANGTHAI_ID, a.CHUQUAN_ID, a.CHUQUAN_ID CHUQUAN_ID_OLD
					, 'vnp_moi' nguon, null tontai_db
		from Ttkd_bsc.dt_ptm_vnp_202410 a
					left join css_hcm.db_thuebao db on db.loaitb_id = 20 and a.somay = '84' || db.ma_tb --and db.ma_tb = '842613371'
					left join css_hcm.db_khachhang kh on db.khachhang_id = kh.khachhang_id
					left join css_hcm.db_thanhtoan tt on db.thanhtoan_id = tt.thanhtoan_id
		where a.chuquan_id = 145
;
----Insert hang thang Sau khi BanKTNV gửi file vao----DS VNPts chuyen tinh về HCM trong thang n (VNPts)
insert into ttkd_bct.dbtb_ttkd_tmp
		select null tb_id, db.KHACHHANG_ID, kh.MA_KH, db.THANHTOAN_ID, db.THUEBAO_ID, a.SOMAY, a.ma_kh MA_TT
							, a.NGAY_LD, null, null,  db.DICHVUVT_ID, db.LOAITB_ID, a.TRANGTHAI_ID, 145 CHUQUAN_ID,  145 CHUQUAN_ID_OLD
							, 'vnp_moi_ct' nguon, null tontai_db
		from ccs_hcm.danhba_dds_102024@ttkddbbk2 a			---thang n
					left join css_hcm.db_thuebao db on db.loaitb_id = 20 and a.somay = '84' || db.ma_tb --and db.ma_tb = '842613371'
					left join css_hcm.db_khachhang kh on db.khachhang_id = kh.khachhang_id
					left join css_hcm.db_thanhtoan tt on db.thanhtoan_id = tt.thanhtoan_id
		where a.ma_kh like 'HCMCT%' and to_char(ngay_ld,'yyyymm') = '202410'		--thang n
;
----Move danh ba hien huu thang n-1 (ngoai tru VNPts)
insert into ttkd_bct.dbtb_ttkd_tmp
select TB_ID, v_db.KHACHHANG_ID, v_db.ma_kh, v_db.THANHTOAN_ID, db.THUEBAO_ID, db.MA_TB, cast(null as varchar(40)) ma_tt
			, v_db.NGAY_SD, v_db.NGAY_TD, v_db.NGAY_CAT, v_db.DICHVUVT_ID, v_db.LOAITB_ID, v_db.TRANGTHAITB_ID, v_db.CHUQUAN_ID, db.CHUQUAN_ID CHUQUAN_ID_OLD
			, cast('onebss_hh' as varchar(300)) nguon
			, nvl2(v_db.thuebao_id, 1, 0) tontai_db
from ttkd_bct.db_thuebao_ttkd db
			left join ttkd_bct.v_db on db.thuebao_id = v_db.thuebao_id
where db.dichvuvt_id not in (2)-- and v_db.CHUQUAN_ID is null
			and not exists (select 1 from ttkd_bct.dbtb_ttkd_tmp where thuebao_id = db.thuebao_id)
;
----Insert hang ngay----DS Khôi Phục trong thang n (ngoai tru VNPts)
insert into ttkd_bct.dbtb_ttkd_tmp
select cast(null as number(15)) tb_id, db.KHACHHANG_ID, kh.MA_KH, db.THANHTOAN_ID, db.THUEBAO_ID, db.MA_TB, tt.MA_TT
			, db.NGAY_SD, db.NGAY_TD, db.NGAY_CAT, db.DICHVUVT_ID, db.LOAITB_ID, db.TRANGTHAITB_ID, db.CHUQUAN_ID
			, db.CHUQUAN_ID CHUQUAN_ID_OLD
			, 'onebss_kp' nguon, nvl2(db.thuebao_id, 1, 0) tontai_db
from  ttkd_bct.v_db db
			left join css_hcm.db_khachhang kh on db.khachhang_id = kh.khachhang_id
			left join css_hcm.db_thanhtoan tt on db.thanhtoan_id = tt.thanhtoan_id
where trunc(db.ngay_kp) between '01/10/2024' and '31/10/2024'
			and db.dichvuvt_id not in (2)
			and not exists (select 1 from ttkd_bct.dbtb_ttkd_tmp where thuebao_id = db.thuebao_id)
;
----Move danh ba hien huu thang n-1 (VNPts)
insert into ttkd_bct.dbtb_ttkd_tmp
select TB_ID, KHACHHANG_ID, MA_KH, THANHTOAN_ID, db.THUEBAO_ID, MA_TB, MA_TT
			, NGAY_SD, NGAY_TD, NGAY_CAT, DICHVUVT_ID, LOAITB_ID, TRANGTHAITB_ID, db.CHUQUAN_ID, db.CHUQUAN_ID CHUQUAN_ID_OLD
			, 'vnp_hh' nguon, null tontai_db
from ttkd_bct.db_thuebao_ttkd db
where dichvuvt_id = 2
;
commit;
---Insert vao bang chinh
truncate table ttkd_bct.dbtb_ttkd;
insert into  ttkd_bct.dbtb_ttkd
	select cast(202410 as number) thang, a.* from ttkd_bct.dbtb_ttkd_tmp a  ---thang n
;
commit;

create or replace view ttkd_bct.v_dbtb_ttkd as
	select a.*, b.mapb_ql, b.donvi_id
	from ttkd_bct.dbtb_ttkd a
				left join ttkd_bct.db_thuebao_ttkd b on a.thuebao_id = b.thuebao_id
;
select * from ttkd_bct.v_dbtb_ttkd;



select donviql_id,count(*) from nguyennc.danhba_dds_102024 group by donviql_id;

update dbtb_ttkd set CHUQUAN_ID='' where dichvuvt_id = 2 and trim(nguon)='vnp_hh';
update dbtb_ttkd a
   set CHUQUAN_ID=145
 where dichvuvt_id = 2 and trim(nguon)='vnp_hh'
   and exists(select * from nguyennc.danhba_dds_102024 where somay=a.ma_tb and ma_kh=a.ma_tt);
 
   
   select CHUQUAN_ID,count(*) from dbtb_ttkd where dichvuvt_id = 2 and trim(nguon)='vnp_hh' group by CHUQUAN_ID ; 
  
    select * from dbtb_ttkd where dichvuvt_id = 2 and CHUQUAN_ID is null; 
 
 
    select * from dbtb_ttkd where dichvuvt_id = 2 and trim(nguon)='vnp_hh' and CHUQUAN_ID is null; 
  
