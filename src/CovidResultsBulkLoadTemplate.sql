bulk insert CovidPennModel
from '${CSV_PATH}.csv'
with (tablock, firstrow=2, fieldterminator=',', rowterminator='\r\n')