# def process_url(file_id: str, outdir: Path, overwrite: bool = False) -> Path | None:
#     # define files and skip if already exists
#     assert outdir.is_dir()
#     segment, segment_number = file_segment_info(file_id)
#     out_file = outdir / segment / f"{segment_number}.parquet"
#     out_file.parent.mkdir(exist_ok=True, parents=True)
#     if not overwrite and out_file.with_suffix(".done").is_file():
#         logger.info(f"[{segment=} number={segment_number}] output file already exists. Skipping...")
#         return
#
#     # process file
#     start = time.perf_counter()
#     logger.info(f"[{segment=} number={segment_number}] Processing file...")
#     try:
#         df = pd.DataFrame(jsonld_generator(file_id))
#     except Exception as e:
#         # write error file
#         err_file = out_file.with_suffix(".error")
#         with open(err_file, "w") as f:
#             f.write(str(e))
#         logger.error(f"[{segment=} number={segment_number}] Error: {e}")
#         time.sleep(1)  # just to ensure we dont gently retry
#         return
#     elapsed = time.perf_counter() - start
#     df.to_parquet(out_file, compression="brotli")
#
#     file_size = os.stat(out_file).st_size
#     logger.info(
#         f"[{segment=} number={segment_number}] Took: {elapsed:.2f} seconds for {len(df)} records"
#     )
#     logger.debug(
#         f"[{segment=} number={segment_number}] Wrote {file_size / 1024**2:.4} MB ({len(df)} recs) to {out_file}"
#     )
#     # write status file and delete error file if successful
#     out_file.with_suffix(".done").touch()
#     if out_file.with_suffix(".error").is_file():
#         out_file.with_suffix(".error").unlink()
#     return out_file

# OLD
