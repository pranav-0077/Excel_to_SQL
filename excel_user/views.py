from django.shortcuts import render
from django.db import transaction, IntegrityError
from django.core.paginator import Paginator
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from .models import ExcelData
import logging
import os
import time
import psutil

logger = logging.getLogger(__name__)

def index(request):
    if request.method == 'POST':
        if 'excel_file' in request.FILES:
            excel_file = request.FILES['excel_file']
            logger.info(f"Received file: {excel_file.name}, size: {excel_file.size} bytes")
            try:
                # Validate file extension
                if not excel_file.name.endswith(('.xls', '.xlsx', '.csv')):
                    logger.error("Invalid file format: File must be .xls, .xlsx, or .csv")
                    return render(request, 'user_excel/excel.html', {
                        'error': 'Invalid file format. Please upload an .xls, .xlsx, or .csv file.'
                    })

                # Validate file size (max 600MB)
                max_size = 600 * 1024 * 1024  # 600MB
                if excel_file.size > max_size:
                    logger.error(f"File too large: {excel_file.size} bytes")
                    return render(request, 'user_excel/excel.html', {
                        'error': f'File is too large. Maximum size is {max_size // (1024 * 1024)}MB.'
                    })

                # Expected columns
                expected_columns = [
                    # Old columns
                    'Voucher Type', 'ID', 'state_name', 'Zone', 'Branch_name', 'Route',
                    'PartyName', 'CategoryName', 'PaymentType', 'CreatedDate', 'VoucherDate',
                    'VoucherNo', 'Bill Type', 'Salesman', 'Taxable', 'CGST', 'SGST', 'IGST',
                    'VoucherAMT', 'Discount', 'Realisable amount', 'RecieveAMT', 'Differance',
                    'RMODE', 'GroupName', 'ItemCOde', 'TaxPerc', 'qty', 'Freeqty',
                    'TotalAmt', 'FreeAmount', 'Rate', 'DiscAmount','Helper 1', 'KL MT OUTLETS', 
                    'TN MT OUTLETS', 'Category', 'NEW SKU','Division', 'Customer name', 
                    'District for milk', 'District for Dashboard','ZONE FOR MT', 'GROUPING FOR ITEM'
                ]


                # Read first row to validate columns
                excel_file.seek(0)
                if excel_file.name.endswith('.csv'):
                    first_df = pd.read_csv(excel_file, nrows=1, dtype_backend='numpy_nullable')
                else:
                    first_df = pd.read_excel(excel_file, nrows=1, dtype_backend='numpy_nullable')
                excel_file.seek(0)
                if not all(col in first_df.columns for col in expected_columns):
                    missing_cols = [col for col in expected_columns if col not in first_df.columns]
                    logger.error(f"Missing columns in file: {missing_cols}")
                    return render(request, 'user_excel/excel.html', {
                        'error': f'Missing required columns: {", ".join(missing_cols)}'
                    })

                # Initialize processing parameters
                chunk_size = 50000  # Increased for faster processing
                total_rows_processed = 0
                invalid_values = []  # Track rows with replaced values
                start_time = time.time()

                # Memory usage tracking
                process = psutil.Process()
                logger.info(f"Initial memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")

                def preprocess_chunk(df):
                    """Preprocess data types in bulk using pandas"""
                    # Convert numeric columns and replace invalid values with 0.0
                    numeric_cols = ['Taxable', 'CGST', 'SGST', 'IGST', 'VoucherAMT', 'Discount',
                                  'Realisable amount', 'RecieveAMT', 'Differance', 'TaxPerc',
                                  'TotalAmt', 'FreeAmount', 'Rate', 'DiscAmount']
                    for col in numeric_cols:
                        if col in df.columns:
                            # Log rows with invalid values
                            invalid_mask = df[col].isna() | df[col].isnull() | (df[col] == '') | (df[col].astype(str).str.strip() == '')
                            if invalid_mask.any():
                                invalid_rows = df[invalid_mask].index.tolist()
                                invalid_values.append({
                                    'column': col,
                                    'rows': [i + 2 for i in invalid_rows],  # +2 for 1-based indexing and header
                                    'values': df.loc[invalid_mask, col].to_list()
                                })
                                logger.warning(f"Invalid values in {col} for rows {invalid_rows}: {df.loc[invalid_mask, col].to_list()}")

                            # Convert to float and replace invalid values with 0.0
                            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float').fillna(0.0)
                            df[col] = df[col].where(df[col].notna(), 0.0)

                    # Convert integer columns and replace invalid values with 0
                    int_cols = ['ID', 'qty', 'Freeqty']
                    for col in int_cols:
                        if col in df.columns:
                            invalid_mask = df[col].isna() | df[col].isnull() | (df[col] == '') | (df[col].astype(str).str.strip() == '')
                            if invalid_mask.any():
                                invalid_rows = df[invalid_mask].index.tolist()
                                invalid_values.append({
                                    'column': col,
                                    'rows': [i + 2 for i in invalid_rows],
                                    'values': df.loc[invalid_mask, col].to_list()
                                })
                                logger.warning(f"Invalid values in {col} for rows {invalid_rows}: {df.loc[invalid_mask, col].to_list()}")

                            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer').fillna(0)
                            df[col] = df[col].where(df[col].notna(), 0)

                    # Convert string columns
                    str_cols = ['Voucher Type', 'state_name', 'Zone', 'Branch_name', 'Route',
                               'PartyName', 'CategoryName', 'PaymentType', 'VoucherNo',
                               'Bill Type', 'Salesman', 'RMODE', 'GroupName', 'ItemCOde''Helper 1', 'KL MT OUTLETS', 
                               'TN MT OUTLETS', 'Category', 'NEW SKU','Division', 'Customer name', 
                               'District for milk', 'District for Dashboard','ZONE FOR MT', 'GROUPING FOR ITEM'
                               ]
                    for col in str_cols:
                        if col in df.columns:
                            df[col] = df[col].astype(str).replace(['nan', 'NaN', '', ' '], None)

                    # Convert date columns
                    for col in ['CreatedDate', 'VoucherDate']:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date
                            # Handle Excel serial dates
                            mask = df[col].isna() & df[col].notna()
                            if mask.any():
                                try:
                                    df.loc[mask, col] = pd.to_numeric(df.loc[mask, col], errors='coerce').apply(
                                        lambda x: (datetime(1899, 12, 30) + timedelta(days=int(x))).date() if pd.notna(x) else None
                                    )
                                except (ValueError, TypeError) as e:
                                    logger.warning(f"Error converting serial dates in {col}: {str(e)}")

                    return df

                # Process file based on type
                if excel_file.name.endswith('.csv'):
                    for chunk in pd.read_csv(excel_file, chunksize=chunk_size, dtype_backend='numpy_nullable'):
                        chunk = preprocess_chunk(chunk)
                        excel_data_objects = []

                        for index, row in chunk.iterrows():
                            try:
                                excel_data = ExcelData(
                                    voucher_type=row.get('Voucher Type'),
                                    sales_id=row.get('ID'),
                                    state_name=row.get('state_name'),
                                    
                                    zone=row.get('Zone'),
                                    branch_name=row.get('Branch_name'),
                                    route=row.get('Route'),
                                    party_name=row.get('PartyName'),
                                    category_name=row.get('CategoryName'),
                                    payment_type=row.get('PaymentType'),
                                    created_date=row.get('CreatedDate'),
                                    voucher_date=row.get('VoucherDate'),
                                    voucher_no=row.get('VoucherNo'),
                                    bill_type=row.get('Bill Type'),
                                    salesman=row.get('Salesman'),
                                    taxable=row.get('Taxable'),
                                    cgst=row.get('CGST'),
                                    sgst=row.get('SGST'),
                                    igst=row.get('IGST'),
                                    voucher_amt=row.get('VoucherAMT'),
                                    discount=row.get('Discount'),
                                    realisable_amount=row.get('Realisable amount'),
                                    receive_amt=row.get('RecieveAMT'),
                                    difference=row.get('Differance'),
                                    rmode=row.get('RMODE'),
                                    group_name=row.get('GroupName'),
                                    item_code=row.get('ItemCOde'),
                                    tax_perc=row.get('TaxPerc'),
                                    qty=row.get('qty'),
                                    free_qty=row.get('Freeqty'),
                                    total_amt=row.get('TotalAmt'),
                                    free_amount=row.get('FreeAmount'),
                                    rate=row.get('Rate'),
                                    disc_amount=row.get('DiscAmount'),
                                    helper_1=row.get('Helper 1'),
                                    kl_mt_outlets=row.get('KL MT OUTLETS'),
                                    tn_mt_outlets=row.get('TN MT OUTLETS'),
                                    new_category=row.get('Category'),
                                    new_sku=row.get('NEW SKU'),
                                    division=row.get('Division'),
                                    customer_name=row.get('Customer name'),
                                    district_milk=row.get('District for milk'),
                                    district_dashboard=row.get('District for Dashboard'),
                                    zone_mt=row.get('ZONE FOR MT'),
                                    grouping_item=row.get('GROUPING FOR ITEM')
                                )
                                excel_data_objects.append(excel_data)
                            except Exception as e:
                                logger.error(f"Error creating ExcelData for row {total_rows_processed + index + 2}: {str(e)}")
                                return render(request, 'user_excel/excel.html', {
                                    'error': f'Error in row {total_rows_processed + index + 2}: {str(e)}'
                                })

                        if excel_data_objects:
                            try:
                                with transaction.atomic():
                                    ExcelData.objects.bulk_create(excel_data_objects, batch_size=5000)
                                    total_rows_processed += len(excel_data_objects)
                                    logger.info(f"Successfully inserted {len(excel_data_objects)} rows in chunk, total inserted: {total_rows_processed}")
                                    logger.info(f"Memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")
                            except IntegrityError as e:
                                logger.error(f"Database error during bulk_create: {str(e)}")
                                return render(request, 'user_excel/excel.html', {
                                    'error': f'Database error during insertion: {str(e)}'
                                })

                else:
                    with pd.ExcelFile(excel_file) as xls:
                        total_rows = pd.read_excel(xls, sheet_name=0, usecols=[0]).shape[0]
                        logger.info(f"Total rows in Excel file: {total_rows}")

                        for start_row in range(0, total_rows, chunk_size):
                            df = pd.read_excel(
                                xls,
                                sheet_name=0,
                                skiprows=start_row,
                                nrows=chunk_size,
                                dtype_backend='numpy_nullable'
                            )
                            df = preprocess_chunk(df)
                            excel_data_objects = []

                            for index, row in df.iterrows():
                                try:
                                    excel_data = ExcelData(
                                        voucher_type=row.get('Voucher Type'),
                                        sales_id=row.get('ID'),
                                        state_name=row.get('state_name'),
                                        zone=row.get('Zone'),
                                        branch_name=row.get('Branch_name'),
                                        route=row.get('Route'),
                                        party_name=row.get('PartyName'),
                                        category_name=row.get('CategoryName'),
                                        payment_type=row.get('PaymentType'),
                                        created_date=row.get('CreatedDate'),
                                        voucher_date=row.get('VoucherDate'),
                                        voucher_no=row.get('VoucherNo'),
                                        bill_type=row.get('Bill Type'),
                                        salesman=row.get('Salesman'),
                                        taxable=row.get('Taxable'),
                                        cgst=row.get('CGST'),
                                        sgst=row.get('SGST'),
                                        igst=row.get('IGST'),
                                        voucher_amt=row.get('VoucherAMT'),
                                        discount=row.get('Discount'),
                                        realisable_amount=row.get('Realisable amount'),
                                        receive_amt=row.get('RecieveAMT'),
                                        difference=row.get('Differance'),
                                        rmode=row.get('RMODE'),
                                        group_name=row.get('GroupName'),
                                        item_code=row.get('ItemCOde'),
                                        tax_perc=row.get('TaxPerc'),
                                        qty=row.get('qty'),
                                        free_qty=row.get('Freeqty'),
                                        total_amt=row.get('TotalAmt'),
                                        free_amount=row.get('FreeAmount'),
                                        rate=row.get('Rate'),
                                        disc_amount=row.get('DiscAmount'),
                                        helper_1=row.get('Helper 1'),
                                        kl_mt_outlets=row.get('KL MT OUTLETS'),
                                        tn_mt_outlets=row.get('TN MT OUTLETS'),
                                        new_category=row.get('Category'),
                                        new_sku=row.get('NEW SKU'),
                                        division=row.get('Division'),
                                        customer_name=row.get('Customer name'),
                                        district_milk=row.get('District for milk'),
                                        district_dashboard=row.get('District for Dashboard'),
                                        zone_mt=row.get('ZONE FOR MT'),
                                        grouping_item=row.get('GROUPING FOR ITEM')
                                    )
                                    excel_data_objects.append(excel_data)
                                except Exception as e:
                                    logger.error(f"Error creating ExcelData for row {start_row + index + 2}: {str(e)}")
                                    return render(request, 'user_excel/excel.html', {
                                        'error': f'Error in row {start_row + index + 2}: {str(e)}'
                                    })

                            if excel_data_objects:
                                try:
                                    with transaction.atomic():
                                        ExcelData.objects.bulk_create(excel_data_objects, batch_size=5000)
                                        total_rows_processed += len(excel_data_objects)
                                        logger.info(f"Successfully inserted {len(excel_data_objects)} rows in chunk, total inserted: {total_rows_processed}")
                                        logger.info(f"Memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")
                                except IntegrityError as e:
                                    logger.error(f"Database error during bulk_create: {str(e)}")
                                    return render(request, 'user_excel/excel.html', {
                                        'error': f'Database error during insertion: {str(e)}'
                                    })

                # Log invalid values and performance
                if invalid_values:
                    logger.warning(f"Replaced invalid values in {len(invalid_values)} columns: {invalid_values}")
                logger.info(f"Total processing time: {time.time() - start_time:.2f} seconds")

                # Check if any data was saved
                saved_count = ExcelData.objects.count()
                if total_rows_processed == 0:
                    logger.error("No rows were processed successfully")
                    return render(request, 'user_excel/excel.html', {
                        'error': 'No data was saved. Please check the file format or data validity.'
                    })

                return render(request, 'user_excel/excel.html', {
                    'message': f'Successfully saved {total_rows_processed} records in {time.time() - start_time:.2f} seconds. Total in database: {saved_count}.'
                })
            except Exception as e:
                logger.error(f"Error processing file: {str(e)}", exc_info=True)
                return render(request, 'user_excel/excel.html', {
                    'error': f'Error processing file: {str(e)}'
                })
        else:
            logger.error("No file uploaded in request")
            return render(request, 'user_excel/excel.html', {
                'error': 'No file uploaded. Please select an Excel or CSV file.'
            })
    
    return render(request, 'user_excel/excel.html')

def view_excel_data(request):
    data_list = ExcelData.objects.all().order_by('-id')
    paginator = Paginator(data_list, 100)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    return render(request, 'user_excel/view_data.html', {
        'page_obj': page_obj,
    })