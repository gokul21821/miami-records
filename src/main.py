"""
Main entry point for the AnyWho phone enrichment tool.
"""

from src.cli.commands import create_parser, process_file, test_enhanced_scraping

def main():
    """Main entry point"""
    parser = create_parser()
    args = parser.parse_args()

    if args.test:
        test_enhanced_scraping()
        return

    # Validate required arguments for non-test mode
    if not args.input_file or not args.output_file:
        parser.error("input_file and output_file are required when not using --test")
    
    process_file(args)

if __name__ == "__main__":
    main()
