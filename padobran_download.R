library(AzureStor)

# downlaod data from Azure blob
blob_key = readLines('./blob_key.txt')
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "padobran")
storage_download(cont, "bonds-predictors.csv", overwrite=TRUE)
