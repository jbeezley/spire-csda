# nasa-csda-cli

This is a CLI and SDK for querying and downloading files from Spire's
[CSDA catalog](https://nasa-csda.wx.spire.com/).

## Install it from PyPI

```bash
pip install nasa-csda
```

## Using the CLI

See the command's help dialog for detailed usage information for all commands.

```bash
nasa-csda-cli --help
```

All of the commands require login information (the same username and password
used to log into the web page). You can also set environment variables to provide
these credentials.

```bash
export CSDA_USERNAME=<username>
export CSDA_PASSWORD=<password>
```

### Bulk downloading files

The catalog's [web page](https://nasa-csda.wx.spire.com/) provides the 
ability to download a query configuration file that can be provided to
the CLI to download all files matching the query created in the UI. To
download all files using this configuration file,

```bash
nasa-csda-cli --username <username> --password <password> bulk-download download-config.json
```

### Querying the catalog

You can also construct queries to perform custom tasks using the `query`
command.

```bash
nasa-csda-cli query --start-date 2020-01-01 --end-date 2020-01-02 \
    --products opnGns,atmPhs \
    --min-latitude -50 --max-latitude 50 --min-longitude -50 --max-longitude 50
```

By default, this will download all matching files in the same way that the bulk
download does. There are two additional modes of operation this command supports.

#### Listing download links

In `list` mode, a link to all files will be printed to STDOUT.

```bash
nasa-csda-cli query --start-date 2020-01-01 --end-date 2020-01-02 \
    --products opnGns,atmPhs \
    --min-latitude -50 --max-latitude 50 --min-longitude -100 --max-longitude 100 \
    --mode list --no-progress --limit 10
```
```
https://nasa-csda.wx.spire.com/download/spire/2020-01-01T23-56-00_FM104_R15_atmPhs/spire_gnss-ro_L1B_atmPhs_v06.01_2020-01-01T23-56-00_FM104_R15.nc
https://nasa-csda.wx.spire.com/download/spire/2020-01-01T23-55-05_FM105_R14_atmPhs/spire_gnss-ro_L1B_atmPhs_v06.01_2020-01-01T23-55-05_FM105_R14.nc
https://nasa-csda.wx.spire.com/download/spire/2020-01-01T23-54-35_FM105_G07_atmPhs/spire_gnss-ro_L1B_atmPhs_v06.01_2020-01-01T23-54-35_FM105_G07.nc
https://nasa-csda.wx.spire.com/download/spire/2020-01-01T23-54-12_FM104_G32_atmPhs/spire_gnss-ro_L1B_atmPhs_v06.01_2020-01-01T23-54-12_FM104_G32.nc
https://nasa-csda.wx.spire.com/download/spire/2020-01-01T23-53-50_FM105_G30_atmPhs/spire_gnss-ro_L1B_atmPhs_v06.01_2020-01-01T23-53-50_FM105_G30.nc
https://nasa-csda.wx.spire.com/download/spire/2020-01-01T23-52-32_FM085_G31_atmPhs/spire_gnss-ro_L1B_atmPhs_v06.01_2020-01-01T23-52-32_FM085_G31.nc
https://nasa-csda.wx.spire.com/download/spire/2020-01-01T23-52-07_FM105_G12_atmPhs/spire_gnss-ro_L1B_atmPhs_v06.01_2020-01-01T23-52-07_FM105_G12.nc
https://nasa-csda.wx.spire.com/download/spire/2020-01-01T23-51-38_FM106_G03_atmPhs/spire_gnss-ro_L1B_atmPhs_v06.01_2020-01-01T23-51-38_FM106_G03.nc
https://nasa-csda.wx.spire.com/download/spire/2020-01-01T23-51-35_FM105_E03_atmPhs/spire_gnss-ro_L1B_atmPhs_v06.01_2020-01-01T23-51-35_FM105_E03.nc
https://nasa-csda.wx.spire.com/download/spire/2020-01-01T23-51-32_FM104_G25_atmPhs/spire_gnss-ro_L1B_atmPhs_v06.01_2020-01-01T23-51-32_FM104_G25.nc
```

#### Getting raw GeoJSON objects

In `raw` mode, the command will stream out GeoJSON objects conforming to the STAC spec.

```bash
nasa-csda-cli query --start-date 2020-01-01 --end-date 2020-01-02 \
    --products opnGns,atmPhs \
    --min-latitude -50 --max-latitude 50 --min-longitude -100 --max-longitude 100 \
    --mode raw --no-progress --limit 1
```

### Using the CLI to authenticate requests

Advanced users can use the `token` command to generate authentication headers that allow
downloading files using other tools.

```bash
TOKEN="$(nasa-csda-cli token)"
curl -O -L -H "Authorization: Bearer ${TOKEN}" https://nasa-csda.wx.spire.com/download/spire/2020-01-01T23-56-00_FM104_R15_atmPhs/spire_gnss-ro_L1B_atmPhs_v06.01_2020-01-01T23-56-00_FM104_R15.nc
```

## Using the SDK

The library can be directly for custom behavior. See the [examples](examples) for information
on how to use it.
