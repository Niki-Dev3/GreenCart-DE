# Configure Azure provider
provider "azurerm" {
  features {}
}

# 1. Resource group
resource "azurerm_resource_group" "greencart_rg" {
  name     = "greencart-rg"
  location = "East US"
}

# 2. Storage account (for raw and processed CSVs)
resource "azurerm_storage_account" "greencart_storage" {
  name                     = "greencartstorage123"  # must be globally unique
  resource_group_name      = azurerm_resource_group.greencart_rg.name
  location                 = azurerm_resource_group.greencart_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

# 3. SQL Database
resource "azurerm_sql_server" "greencart_sql" {
  name                         = "greencart-sql"
  resource_group_name          = azurerm_resource_group.greencart_rg.name
  location                     = azurerm_resource_group.greencart_rg.location
  version                      = "12.0"
  administrator_login          = "sqladmin"
  administrator_login_password = "P@ssword123!"
}

resource "azurerm_sql_database" "greencart_db" {
  name                = "greencartdb"
  resource_group_name = azurerm_resource_group.greencart_rg.name
  location            = azurerm_resource_group.greencart_rg.location
  server_name         = azurerm_sql_server.greencart_sql.name
  sku_name            = "Basic"
}
