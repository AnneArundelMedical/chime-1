bulk insert CovidModel.dbo.CovidPennModel
from '${CSV_PATH}'
with (tablock, firstrow=2, fieldterminator=',', rowterminator='\r\n')