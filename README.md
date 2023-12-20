# tcat-dash

Create a dashboard that shows details about service for the previous day.
Provide a functionality to enter a date range to present the same details over time.

Data sources:
- daily avail vhist queries show all historical vehicle data.
- daily genfare queries show farebox events from all buses that collected fares.
- daily FuelMaster reports fuel consumed and miles traveled.
- Swiftly actual service reports help fill in the blanks and show how the other
 systems aren't reporting properly.


Visual components to be considered:
    bus list with statistics and metadata
        - miles traveled
        - fuel used

    route list with revenue mileage and passengers
    trips completed and missed
    passenger fare breakdown
        all fare types/cash fares
        Cornell fare breakdown

Key metrics to report:

Which buses ran?
    - which buses ran on which routes?
        a) by extrapolation
            how many miles were traveled on each route
            how many passenger trips on each route
        b) which trips were completed?
            total up revenue trip mileage
        c) which trips were missed?
            total up missed trip mileage

