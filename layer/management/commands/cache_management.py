from django.core.management.base import BaseCommand

from layer.cache_utils import (
    clear_all_caches,
    invalidate_data_caches,
    invalidate_geography_caches,
    invalidate_indicator_caches,
)


class Command(BaseCommand):
    help = 'Manage application caches'

    def add_arguments(self, parser):
        parser.add_argument(
            '--clear-all',
            action='store_true',
            help='Clear all caches',
        )
        parser.add_argument(
            '--invalidate-data',
            action='store_true',
            help='Invalidate data-dependent caches',
        )
        parser.add_argument(
            '--invalidate-geography',
            action='store_true',
            help='Invalidate geography-dependent caches',
        )
        parser.add_argument(
            '--invalidate-indicators',
            action='store_true',
            help='Invalidate indicator-dependent caches',
        )

    def handle(self, *args, **options):
        if options['clear_all']:
            self.stdout.write('Clearing all caches...')
            clear_all_caches()
            self.stdout.write(self.style.SUCCESS('All caches cleared successfully'))

        if options['invalidate_data']:
            self.stdout.write('Invalidating data caches...')
            invalidate_data_caches()
            self.stdout.write(self.style.SUCCESS('Data caches invalidated'))

        if options['invalidate_geography']:
            self.stdout.write('Invalidating geography caches...')
            invalidate_geography_caches()
            self.stdout.write(self.style.SUCCESS('Geography caches invalidated'))

        if options['invalidate_indicators']:
            self.stdout.write('Invalidating indicator caches...')
            invalidate_indicator_caches()
            self.stdout.write(self.style.SUCCESS('Indicator caches invalidated'))

        if not any([
            options['clear_all'],
            options['invalidate_data'],
            options['invalidate_geography'],
            options['invalidate_indicators']
        ]):
            self.stdout.write(self.style.WARNING(
                'No action specified. Use --help to see available options.'
            ))
