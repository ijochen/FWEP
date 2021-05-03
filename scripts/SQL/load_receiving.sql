--select version()
--select warehouse.load_receiving()

--drop function warehouse.load_receiving()



CREATE FUNCTION warehouse.load_receiving()  RETURNS void AS $$
begin
	 
	declare 
		start_date_var timestamp := cast('1/7/2021' as timestamp);
	begin

		-- Clear out old temp tables
		truncate table warehouse.receiving_ins;
		truncate table warehouse.receiving_outs;
		truncate table warehouse.temp_receiving_results;
		truncate table warehouse.temp_receiving_locations;
		truncate table warehouse.temp_receiving_invt_ids;
		truncate table warehouse_temp_receiving_in_refs;
		truncate table warehouse_temp_receiving_product_rf_out_rows;
		truncate table warehouse.temp_receiving_sites;

		-- In refs
		insert into warehouse.receiving_ins
		select 
			transaction_number as record_id,
			bin as warehouse_bin_location,
			inv_mast_uid as invt_id,
            item_id,
			item_desc, 
			quantity,
			date_created as Crtd_DateTime,
			'in' as direction,
			cast(location_id as varchar(100)) as site_id
		from warehouse.inv_transactions
		where 
			quantity > 0 and 
			date_created >= start_date_var and 
			location_id = 140
		order by date_created desc;

		-- Out refs
		insert into warehouse.receiving_outs
		select
			transaction_number as record_id,
			bin as warehouse_bin_location,
			inv_mast_uid as invt_id,
            item_id,
			item_desc, 
			quantity,
			date_created as Crtd_DateTime,
			'out' as direction,
			cast(location_id as varchar(100)) as site_id
		from warehouse.inv_transactions
		where  
			quantity < 0 and 
			date_created >= start_date_var and 
			location_id = 140
		order by date_created desc;
	end;

	-- For every site 
	insert into warehouse.temp_receiving_sites
	select distinct cast(site_id as varchar(100))
	from warehouse.receiving_outs;
	declare
		site_ct_var integer := (select count(*) from warehouse.temp_receiving_sites);
	begin 
		WHILE site_ct_var > 0 loop

			declare 
				cur_site_var varchar(100) := (select site_id from warehouse.temp_receiving_sites limit 1);
			begin

				-- For every location
				INSERT INTO warehouse.temp_receiving_locations (warehouse_bin_location)
				select distinct warehouse_bin_location from warehouse.receiving_outs where site_id = cur_site_var;
				declare
					loc_ct_var integer := (select count(*) from warehouse.temp_receiving_locations);
				begin
					
					WHILE loc_ct_var > 0 loop -- Here down!

						DECLARE 
							cur_loc_var varchar(100) := (select warehouse_bin_location from warehouse.temp_receiving_locations limit 1);
						BEGIN

							--For every invt_id
							INSERT INTO warehouse.temp_receiving_invt_ids (invt_id)
							select distinct invt_id from warehouse.receiving_outs 
							where warehouse_bin_location = cur_loc_var and site_id = cur_site_var;

							DECLARE 
								invt_ct_var integer := (select count(*) from warehouse.temp_receiving_invt_ids);
							begin 

								WHILE invt_ct_var > 0 loop

									DECLARE 
										cur_invt_id_var integer := (select invt_id from warehouse.temp_receiving_invt_ids limit 1);
									begin

										INSERT INTO warehouse.temp_receiving_in_refs (record_id, quantity, crtd_datetime)
										select 
											record_id, quantity, Crtd_DateTime 
										from warehouse.receiving_ins 
										where invt_id = cur_invt_id_var and warehouse_bin_location = cur_loc_var and site_id = cur_site_var order by Crtd_DateTime desc; -- TODO: Is desc right?
									
										declare
											cur_in_ref_record_id_var integer := (select record_id from warehouse.temp_receiving_in_refs limit 1);
											cur_in_ref_qty_var integer := (select quantity from warehouse.temp_receiving_in_refs limit 1);
											cur_in_ref_date_var timestamp := (select Crtd_DateTime from warehouse.temp_receiving_in_refs limit 1);
										begin 

											-- While IN refs (allocate to) AND OUT refs (allocate from)
											insert into warehouse.temp_receiving_product_rf_out_rows (quantity, record_id)
											select quantity, record_id 
											from warehouse.receiving_outs 
											where invt_id = cur_invt_id_var and warehouse_bin_location = cur_loc_var and site_id = cur_site_var order by Crtd_DateTime desc;

											DECLARE 
												cur_in_remainder_var integer := cur_in_ref_qty_var;
											begin

												while (select count(*) from warehouse.temp_receiving_in_refs) > 0 and (select count(*) from warehouse.temp_receiving_product_rf_out_rows) > 0 loop

													DECLARE
														DECLARE cur_product_rf_out_qty_var integer := (select quantity * -1 from warehouse.temp_receiving_product_rf_out_rows limit 1); -- Need to reverse the negative quantity
														DECLARE cur_product_rf_out_record_id_var integer := (select record_id from warehouse.temp_receiving_product_rf_out_rows limit 1);
													begin

														-- case 1: cur_in_remainder > cur_product_rf_out.qty
														if cur_in_remainder_var >= cur_product_rf_out_qty_var then

															INSERT INTO warehouse.temp_receiving_results (record_id, in_record_id, quantity) 
															values (cur_product_rf_out_record_id_var, cur_in_ref_record_id_var, cur_product_rf_out_qty_var);
														
															cur_in_remainder_var := cur_in_remainder_var - cur_product_rf_out_qty_var;
											
															-- case 2: cur_in_remainder = cur_product_rf_out
															if cur_in_remainder_var = 0 then
																-- Go to next in_ref
																delete from warehouse.temp_receiving_in_refs where record_id = cur_in_ref_record_id_var;
																cur_in_ref_record_id_var := (select record_id from warehouse.temp_receiving_in_refs limit 1);
																cur_in_ref_qty_var := (select quantity from warehouse.temp_receiving_in_refs limit 1);
																cur_in_ref_date_var := (select Crtd_DateTime from warehouse.temp_receiving_in_refs limit 1);
																cur_in_remainder_var := cur_in_ref_qty_var;
															end if;
														
															delete from warehouse.temp_receiving_product_rf_out_rows where record_id = cur_product_rf_out_record_id_var; 
													
														-- case 3: cur_in_remainder < cur_product_rf_out 
														ELSE --if @cur_in_remainder < @cur_product_rf_out_qty
															INSERT INTO warehouse.temp_receiving_results (record_id, in_record_id, quantity) 
															values (cur_product_rf_out_record_id_var, cur_in_ref_record_id_var, cur_in_remainder_var);
														
															UPDATE warehouse.temp_receiving_product_rf_out_rows SET quantity = (cur_product_rf_out_qty_var - cur_in_remainder_var) * -1.0
															where record_id = cur_product_rf_out_record_id_var;

															-- Go to next in_ref
															delete from warehouse.temp_receiving_in_refs where record_id = cur_in_ref_record_id_var;
															cur_in_ref_record_id_var := (select record_id from warehouse.temp_receiving_in_refs limit 1);
															cur_in_ref_qty_var := (select quantity from warehouse.temp_receiving_in_refs limit 1);
															cur_in_ref_date_var := (select Crtd_DateTime from warehouse.temp_receiving_in_refs limit 1);
															cur_in_remainder_var := cur_in_ref_qty_var;
														end if;

													end;
													
												END loop;

											end;

										end;

										-- Clear out if there are any remainders
										truncate table warehouse.temp_receiving_in_refs;
										truncate table warehouse.temp_receiving_product_rf_out_rows;

										-- End Loop Invt-Id
										delete from warehouse.temp_receiving_invt_ids where invt_id = cur_invt_id_var;
										invt_ct_var := (select count(*) from warehouse.temp_receiving_invt_ids);

									end;

								END loop;

							end;

							-- End Loop Location
							delete from warehouse.temp_receiving_locations where warehouse_bin_location = cur_loc_var;
							loc_ct_var := (select count(*) from warehouse.temp_receiving_locations);

						end;

					END loop;

				end;

				-- End Loop Location	
				delete from warehouse.temp_receiving_sites where site_id = cur_site_var;
				site_ct_var := (select count(*) from warehouse.temp_receiving_sites);

			end;
		

		end loop;
	end;

	---- FULL RESULTS
	 truncate table warehouse.receiving;
	 insert into warehouse.receiving
	 select * 
	 from (
	 	select 
	 		record_id as in_record_id,
	 		-1 as out_record_id,
	 		'IN' as direction,
	 		warehouse_bin_location,
	 		invt_id,
            item_id,
			item_desc, 
	 		quantity,
	 		Crtd_DateTime,
	 		site_id
	 	from warehouse.receiving_ins
	 	union all 
	
	 	select
	 		m.in_record_id,
	 		o.record_id as out_record_id,
	 		'OUT' as direction,
	 		o.warehouse_bin_location,
	 		o.invt_id,
            o.item_id,
			o.item_desc, 
	 		case
	 			when m.in_record_id is not null then m.quantity * -1
	 			else o.quantity
	 		end as quantity, 
	 		--m.quantity * -1,
	 		o.Crtd_DateTime,
	 		site_id
	 	from warehouse.receiving_outs o 
	 	left join warehouse.temp_receiving_results m 
	 		on m.record_id = o.record_id
	 ) a;

END ;
$$
LANGUAGE plpgsql ;
