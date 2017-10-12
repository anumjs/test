--Altering tables and views to include rowversion and customerEmailId as the id 

-- Create job to retrieve analytics that are distributed across all the tenants
EXEC jobs.sp_add_job
@job_name='ModifyTableViewTimestamp',
@description='Retrieve tickets data from all tenants',
@enabled=1,
@schedule_interval_type='Once'
--@schedule_interval_type='Minutes',
--@schedule_interval_count=15,
--@schedule_start_time='2017-08-21 10:00:00.0000000',
--@schedule_end_time='2017-08-21 11:00:00.0000000'


EXEC jobs.sp_add_jobstep
@job_name='ModifyTableViewTimestamp',
@command=N'
ALTER TABLE [dbo].[TicketPurchases]
ADD RowVersion rowversion
GO 
 
ALTER TABLE [dbo].[Events]
ADD RowVersion rowversion
GO 

ALTER TABLE [dbo].[Venue]
ADD RowVersion rowversion
GO

ALTER VIEW [dbo].[VenueEvents] AS
    SELECT (SELECT TOP 1 VenueId FROM Venues) AS VenueId, EventId, EventName, Subtitle, RowVersion, Date FROM [events]
GO

ALTER VIEW [dbo].[TicketFacts] AS
    SELECT  v.VenueId, v.VenueName, v.VenueType,v.VenuePostalCode, v.VenueCapacity,
            tp.TicketPurchaseId, tp.PurchaseDate, tp.PurchaseTotal,
            t.RowNumber, t.SeatNumber, 
            Convert(int, HASHBYTES(''md5'', c.Email)) AS CustomerEmailId, c.PostalCode AS CustomerPostalCode, c.CountryCode, 
            e.EventId, e.EventName, e.Subtitle AS EventSubtitle, e.Date AS EventDate, tp.RowVersion
    FROM    (
              SELECT  (SELECT TOP 1 VenueId FROM [dbo].[Venues]) AS VenueId,
                      VenueName, VenueType, PostalCode AS VenuePostalCode,
                      (SELECT SUM ([SeatRows]*[SeatsPerRow]) FROM [dbo].[Sections]) AS VenueCapacity, 
                      1 AS X FROM Venue
            ) as v
            INNER JOIN [dbo].[TicketPurchases] AS tp ON v.X = 1
            INNER JOIN [dbo].[Tickets] AS t ON t.TicketPurchaseId = tp.TicketPurchaseId
            INNER JOIN [dbo].[Events] AS e ON t.EventId = e.EventId
            INNER JOIN [dbo].[Customers] AS c ON tp.CustomerId = c.CustomerId
GO

ALTER VIEW [dbo].[Venues]
AS
SELECT Convert(int, HASHBYTES(''md5'',VenueName)) AS VenueId, VenueName, VenueType, AdminEmail, PostalCode, CountryCode, @@ServerName as Server, DB_NAME() AS [DatabaseName], RowVersion FROM [Venue]

',
@credential_name='mydemocred',
@target_group_name='TenantGroup'


--
-- Views
-- Job and Job Execution Information and Status

--View all execution status
SELECT * FROM [jobs].[job_executions] 
WHERE job_name = 'ModifyTableViewTimestamp'

-- Cleanup
--EXEC [jobs].[sp_delete_job] 'ModifyTableViewTimestamp'
--EXEC [jobs].[sp_delete_target_group] 'TenantGroupCS'

--EXEC jobs.sp_start_job 'ModifyTicketFactsView'