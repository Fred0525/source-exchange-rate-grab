#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_exchange_rate_grab import SourceExchangeRateGrab

if __name__ == "__main__":
    source = SourceExchangeRateGrab()
    launch(source, sys.argv[1:])
