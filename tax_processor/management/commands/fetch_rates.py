import zeep
from decimal import Decimal
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from tax_processor.models import ExchangeRate

# The URL for the Central Bank of Armenia's SOAP API
CBA_WSDL_URL = 'https://api.cba.am/exchangerates.asmx?WSDL'

# The currencies we want to store.
CURRENCIES_TO_FETCH = ['USD', 'EUR', 'RUB', 'GBP']

class Command(BaseCommand):
    help = 'Fetches historical exchange rates from the Central Bank of Armenia (CBA) for a given date range.'

    def add_arguments(self, parser):
        parser.add_argument('start_date', type=str, help='Start date in YYYY-MM-DD format')
        parser.add_argument('end_date', type=str, help='End date in YYYY-MM-DD format')

    def handle(self, *args, **options):
        self.stdout.write(self.style.NOTICE(f"Connecting to CBA API at {CBA_WSDL_URL}..."))
        try:
            client = zeep.Client(CBA_WSDL_URL)
            self.stdout.write(self.style.SUCCESS("Successfully connected to CBA API."))
        except Exception as e:
            raise CommandError(f"Could not connect to Zeep client: {e}")

        try:
            start_date = datetime.strptime(options['start_date'], '%Y-%m-%d').date()
            end_date = datetime.strptime(options['end_date'], '%Y-%m-%d').date()
        except ValueError:
            raise CommandError("Invalid date format. Please use YYYY-MM-DD.")

        if end_date > timezone.now().date():
            end_date = timezone.now().date()
            self.stdout.write(self.style.WARNING(f"End date is in the future. Setting to today: {end_date}"))

        current_date = start_date
        total_rates_saved = 0

        self.stdout.write(f"Fetching rates from {start_date} to {end_date}...")

        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%dT00:00:00')
            self.stdout.write(f"  Fetching for date: {current_date.strftime('%Y-%m-%d')}")

            try:
                # Call the API method
                result = client.service.ExchangeRatesByDate(date_str)

                if not result or not result.Rates or not result.Rates.ExchangeRate:
                    self.stdout.write(self.style.WARNING(f"    No data returned for {current_date} (possibly a weekend or holiday)."))
                    current_date += timedelta(days=1)
                    continue

                rates_found_for_day = 0
                for rate_data in result.Rates.ExchangeRate:

                    # --- THIS IS THE FIX ---
                    # Use the exact capitalization from the debug log
                    iso_code = rate_data.ISO.upper()
                    # --- END FIX ---

                    if iso_code in CURRENCIES_TO_FETCH:
                        try:
                            # --- THIS IS THE FIX ---
                            # Use the exact capitalization from the debug log
                            rate_value = Decimal(rate_data.Rate)
                            per_unit_amount = Decimal(rate_data.Amount)
                            # --- END FIX ---

                            if per_unit_amount == 0:
                                continue # Avoid division by zero

                            normalized_rate = rate_value / per_unit_amount

                            # Save to database
                            obj, created = ExchangeRate.objects.update_or_create(
                                date=current_date,
                                currency_code=iso_code,
                                defaults={'rate': normalized_rate}
                            )

                            if created:
                                rates_found_for_day += 1
                                total_rates_saved += 1

                        except Exception as e:
                            self.stdout.write(self.style.ERROR(f"    Error processing rate for {iso_code}: {e}"))

                self.stdout.write(f"    Saved {rates_found_for_day} new rates for this day.")

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"  Failed to fetch data for {current_date}: {e}"))

            # Move to the next day
            current_date += timedelta(days=1)

        self.stdout.write(self.style.SUCCESS(f"\nFinished. Successfully saved {total_rates_saved} new exchange rates."))
