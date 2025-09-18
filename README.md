# growth-intel-V2

**Quick note:**
In the folder _bq queries_ two separate SQL files have been added: _base table_ and _V2 indicators_. This separation has been done to make the underlying logic simple to understand. For implementation purposes, the former can be used as a CTE in the latter script.

### Must-have changes:
Indicator logic from V2_indicators, which includes:

    * Land use:
        * Include if Year Built is NULL OR address is vacant (vacancy>0)
        * Exclude in all other cases

    * New Clip
        * Same as above (land use)

    * New Construction Permit Indicator
        * Same as above (land use) 

    * Builder Developer
        * Exclude if property was sold 5+ years ago OR 
        * Exclude if built more than 3 years ago and is occupied/not vacant
        * Include in all other cases


### Good-to-have changes:
In _base tables_ sql:

    * Exclude clips that do not have a polygon/multi-polygon geometry

    * Take the max value of COALESCED(year built, effective year built) instead of applying buildingseqnum = 1 logic
    
    * Take COALESCE(COUNT(year built), COUNT(effective year built)) as number of structures