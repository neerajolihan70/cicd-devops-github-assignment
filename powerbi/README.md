# Power BI Dashboard

## Status: Skipped (Linux Environment)

Power BI Desktop requires Windows. This project was built on Linux, so the Power BI component is not included.

See [LINUX_ALTERNATIVES.md](LINUX_ALTERNATIVES.md) for why this is acceptable.

---

## If You Have Windows

Quick setup:

1. **Get Data** → PostgreSQL database
2. **Server**: `your-redshift-endpoint:5439`
3. **Database**: `analytics`
4. **Table**: `curated.sales_orders`

### Sample Measures

```dax
Total Revenue = SUM(sales_orders[amount])
Order Count = COUNT(sales_orders[order_id])
Avg Order Value = DIVIDE([Total Revenue], [Order Count])
```

### Suggested Visuals

- KPI cards for revenue, orders, avg value
- Line chart: sales trend by date
- Bar chart: top customers by revenue

Save as `.pbip` format when done.
- Click **Refresh** button in Power BI Desktop

### Scheduled Refresh (Power BI Service)
1. Go to Power BI Service
2. Find your dataset
3. **Settings** → **Scheduled refresh**
4. Configure:
   - Frequency: Daily
   - Time: After Airflow DAG runs
   - Time zone: Europe/London

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to Redshift

**Solutions**:
1. Check Redshift endpoint is correct
2. Verify port 5439 is accessible
3. Check security group allows your IP
4. Verify VPC public subnet or VPN
5. Test with: `telnet endpoint 5439`

### Authentication Errors

**Problem**: Invalid username/password

**Solutions**:
1. Verify credentials in AWS console
2. Check case-sensitivity
3. Try resetting Redshift password

### Performance Issues

**Problem**: Slow query performance

**Solutions**:
1. Use **Import** instead of DirectQuery
2. Limit date range with filters
3. Create aggregated tables in Redshift
4. Add indexes/sort keys in Redshift

### Data Not Updating

**Problem**: Old data showing

**Solutions**:
1. Click **Refresh** in Power BI
2. Check Airflow DAG ran successfully
3. Verify data in Redshift directly:
   ```sql
   SELECT COUNT(*), MAX(order_date)
   FROM curated.sales_orders;
   ```

## Best Practices

1. **Use Import for static data**: Faster performance
2. **Use DirectQuery for real-time**: When data changes frequently
3. **Limit data**: Use WHERE clauses to filter at source
4. **Aggregate in Redshift**: Create summary tables
5. **Test performance**: Monitor query execution times
6. **Document measures**: Add descriptions to DAX measures
7. **Version control**: Save .pbip in Git

## Sample DAX Queries

### Query Editor (M)

```m
let
    Source = PostgreSQL.Database("your-endpoint:5439", "dev"),
    curated_sales_orders = Source{[Schema="curated",Item="sales_orders"]}[Data],
    #"Changed Type" = Table.TransformColumnTypes(curated_sales_orders,{{"order_date", type date}, {"amount", type number}})
in
    #"Changed Type"
```

### Filter Recent Data

```m
let
    Source = PostgreSQL.Database("your-endpoint:5439", "dev"),
    curated_sales_orders = Source{[Schema="curated",Item="sales_orders"]}[Data],
    #"Filtered Rows" = Table.SelectRows(curated_sales_orders, each [order_date] >= #date(2024, 1, 1))
in
    #"Filtered Rows"
```

## Report Requirements Checklist

- ✅ Connect to Redshift curated tables
- ✅ Minimum 3 DAX measures
- ✅ At least 1 visualization (chart/graph)
- ✅ Clear, professional layout
- ✅ Saved as .pbip project
- ✅ Documentation for setup

## Screenshots to Capture

1. Connection dialog with Redshift endpoint
2. Data model view showing tables
3. Measure definitions in DAX
4. Final dashboard/report
5. Refresh dialog showing successful update

## Additional Resources

- [Power BI Documentation](https://docs.microsoft.com/en-us/power-bi/)
- [DAX Reference](https://dax.guide/)
- [Redshift ODBC Driver](https://docs.aws.amazon.com/redshift/latest/mgmt/configure-odbc-connection.html)
- [Power BI Community](https://community.powerbi.com/)
