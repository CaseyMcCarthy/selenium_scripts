import json
from boto3 import resource
from decimal import Decimal
from datetime import datetime
import uuid
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

payload = [
    {
        "site_id": "3af08cc2-597e-11ea-ad6a-0e9d39187912",
        "listing_id": "5e7e754a18a634002d4da6bf",
        "state": "Utah",
        "park": "East Canyon State Park",
        "name": "Wagon 1",
        "ra_name": "Tentrr 008",
        "format": "multiple_roles"
    },
    {
        "site_id": "5925a998-597e-11ea-ad6a-0e9d39187912",
        "listing_id": "5e87e0f86299d10029486dda",
        "state": "Utah",
        "park": "East Canyon State Park",
        "name": "Wagon 2",
        "ra_name": "Tentrr 009",
        "format": "multiple_roles"
    },
    {
        "site_id": "9874e208-597e-11ea-ad6a-0e9d39187912",
        "listing_id": "5e7e753deef56b0028227eb5",
        "state": "Utah",
        "park": "East Canyon State Park",
        "name": "Wagon 3",
        "ra_name": "Tentrr 010",
        "format": "multiple_roles"
    },
    {
        "site_id": "21269de0-597e-11ea-ad6a-0e9d39187912",
        "listing_id": "5e7e74fcdc0baa002b3d2f21",
        "state": "Utah",
        "park": "East Canyon State Park",
        "name": "Peninsula Site 1",
        "ra_name": "Tentrr 007",
        "format": "multiple_roles"
    },
    {
        "site_id": "38015452-597d-11ea-ad6a-0e9d39187912",
        "listing_id": "5e7e74ccb54289002c21de7b",
        "state": "Utah",
        "park": "East Canyon State Park",
        "name": "Mormon Flats Site 1",
        "ra_name": "Tentrr 001",
        "format": "multiple_roles"
    },
    {
        "site_id": "9b16c6c6-597d-11ea-ad6a-0e9d39187912",
        "listing_id": "5e7e7513e752f6002ca28cb4",
        "state": "Utah",
        "park": "East Canyon State Park",
        "name": "Mormon Flats Site 2",
        "ra_name": "Tentrr 002",
        "format": "multiple_roles"
    }
]
    ,
    {
        "site_id": "08ef9492-62f3-11ea-ad6a-0e9d39187912",
        "listing_id": "5eb4851e3d4953002d447eb9",
        "state": "Utah",
        "park": "Wasatch Mountain State Park",
        "name": "Duck's Landing ADA",
        "ra_name": "Tentrr Ducks Landing"
    },
    {
        "site_id": "057c41d4-62f3-11ea-a65e-0e1671378d08",
        "listing_id": "5eb48523c280cd002b985eae",
        "state": "Utah",
        "park": "Wasatch Mountain State Park",
        "name": "Oak Hollow Site 116",
        "ra_name": "Tentrr Oak Hollow Site #116"
    },
    {
        "site_id": "b4104ef6-6542-11ea-ad6b-0e9d39187912",
        "listing_id": "5eb4851a99f466002990ff18",
        "state": "Utah",
        "park": "Wasatch Mountain State Park",
        "name": "Rabbit Den",
        "ra_name": "Tentrr Rabbit Den"
    },
    {
        "site_id": "4fdc2202-6a10-11ea-ad6c-0e9d39187912",
        "listing_id": "5eb485128b5860002d7d832f",
        "state": "Utah",
        "park": "Fred Hayes State Park at Starvation",
        "name": "Single Site 1",
        "ra_name": "Tentrr 001"
    },
    {
        "site_id": "5546172a-6a10-11ea-ad6c-0e9d39187912",
        "listing_id": "5eb4851c3b89890029f0a2cb",
        "state": "Utah",
        "park": "Fred Hayes State Park at Starvation",
        "name": "Single Site 2",
        "ra_name": "Tentrr 002"
    },
    {
        "site_id": "57f14972-6a10-11ea-a65e-0e1671378d08",
        "listing_id": "5eb4851ff8ec90002dced0a0",
        "state": "Utah",
        "park": "Fred Hayes State Park at Starvation",
        "name": "Single Site 3",
        "ra_name": "Tentrr 003"
    },
    {
        "site_id": "f0aaea6e-5ef4-11ea-ad6a-0e9d39187912",
        "listing_id": "5e7e747f28e18b003160a629",
        "state": "Utah",
        "park": "Red Fleet State Park",
        "name": "Waterfront Double Campsite",
        "ra_name": "Tentrr Reservoir Site 001"
    },
    {
        "site_id": "ff752ad8-5fe3-11ea-a65e-0e1671378d08",
        "listing_id": "5e7e754018a634002d4da4bd",
        "state": "Utah",
        "park": "Steinaker State Park",
        "name": "Campsite 1",
        "ra_name": "Tentrr 001"
    },
    {
        "site_id": "5abb1d90-623d-11ea-ad6a-0e9d39187912",
        "listing_id": "5e7e74adeef56b002822719a",
        "state": "Utah",
        "park": "Steinaker State Park",
        "name": "Double Campsite",
        "ra_name": "Tentrr 010"
    },
    {
        "site_id": "fa7ebd18-5fe4-11ea-a65e-0e1671378d08",
        "listing_id": "5e7e754a42ea20002805baa7",
        "state": "Utah",
        "park": "Steinaker State Park",
        "name": "Campsite 2",
        "ra_name": "Tentrr 002"
    },
    {
        "site_id": "4c829f48-2525-11eb-adc0-0e9d39187912",
        "listing_id": "5fadae150933cb002d0702f2",
        "state": "Utah",
        "park": "Kodachrome Basin State Park",
        "name": "Site A",
        "ra_name": "A"
    },
    {
        "site_id": "3af39a8e-2525-11eb-8a96-0e1671378d08",
        "listing_id": "5fadb34dc7525f003218c3e6",
        "state": "Utah",
        "park": "Kodachrome Basin State Park",
        "name": "Site B",
        "ra_name": "B"
    },
    {
        "site_id": "49fbe392-2525-11eb-8a96-0e1671378d08",
        "listing_id": "5fadc137c7525f003219af01",
        "state": "Utah",
        "park": "Kodachrome Basin State Park",
        "name": "Site C",
        "ra_name": "C"
    },
    {
        "site_id": "4ed2c2be-2525-11eb-8a96-0e1671378d08",
        "listing_id": "5fadc0111bef94003021d72a",
        "state": "Utah",
        "park": "Kodachrome Basin State Park",
        "name": "Site D",
        "ra_name": "D"
    },
    {
        "site_id": "402eeb98-2525-11eb-8a96-0e1671378d08",
        "listing_id": "5fadc4c5c7525f00321a240b",
        "state": "Utah",
        "park": "Kodachrome Basin State Park",
        "name": "Site E",
        "ra_name": "E"
    },
    {
        "site_id": "5156b950-2525-11eb-adc0-0e9d39187912",
        "listing_id": "5fadbf612e3eb5002da00e98",
        "state": "Utah",
        "park": "Kodachrome Basin State Park",
        "name": "Site F",
        "ra_name": "F"
    },
    {
        "site_id": "3de08f04-2525-11eb-adc0-0e9d39187912",
        "listing_id": "5fadc55a54eb5e002f9e9e08",
        "state": "Utah",
        "park": "Kodachrome Basin State Park",
        "name": "Site G",
        "ra_name": "G"
    },
    {
        "site_id": "479fa11a-2525-11eb-adc0-0e9d39187912",
        "listing_id": "5fadc1e11bef94003021ec55",
        "state": "Utah",
        "park": "Kodachrome Basin State Park",
        "name": "Site H",
        "ra_name": "H"
    },
    {
        "site_id": "45673fac-2525-11eb-8a96-0e1671378d08",
        "listing_id": "5fadc3652e3eb5002da07951",
        "state": "Utah",
        "park": "Kodachrome Basin State Park",
        "name": "Site I",
        "ra_name": "I"
    },
    {
        "site_id": "42e7a6f4-2525-11eb-adc0-0e9d39187912",
        "listing_id": "5fadc40a9ba0c1002fbd8aff",
        "state": "Utah",
        "park": "Kodachrome Basin State Park",
        "name": "Site J",
        "ra_name": "J"
    },
    {
        "site_id": "8458ac9a-2535-11eb-adc0-0e9d39187912",
        "listing_id": "5fadca2730748c0030cf86ef",
        "state": "Utah",
        "park": "Otter Creek State Park",
        "name": "Double Campsite A",
        "ra_name": "Site A"
    },
    {
        "site_id": "8ba5f408-2535-11eb-8a96-0e1671378d08",
        "listing_id": "5fadc9ac1bef940030228307",
        "state": "Utah",
        "park": "Otter Creek State Park",
        "name": "Single Site B",
        "ra_name": "Site B"
    },
    {
        "site_id": "8e8a5f74-2535-11eb-adc0-0e9d39187912",
        "listing_id": "5fadc90554eb5e002f9ed41c",
        "state": "Utah",
        "park": "Otter Creek State Park",
        "name": "Double Site C",
        "ra_name": "Site C"
    }
]
#     {
#         "site_id": "7e71ee28-0e5a-11eb-8a95-0e1671378d08",
#         "listing_id": "5f90738e8589a20026dc759d",
#         "state": "Louisiana",
#         "park": "Fontainebleau State Park",
#         "name": "Lakeside 1",
#         "ra_name": "Site A"
#     },
#     {
#         "site_id": "c49e8906-0e5a-11eb-8a95-0e1671378d08",
#         "listing_id": "5f999d87809958002ef45757",
#         "state": "Louisiana",
#         "park": "Fontainebleau State Park",
#         "name": "Lakeside 2",
#         "ra_name": "Site B"
#     },
#     {
#         "site_id": "b60e0164-0e5a-11eb-adbd-0e9d39187912",
#         "listing_id": "5f999d87809958002ef456e0",
#         "state": "Louisiana",
#         "park": "Fontainebleau State Park",
#         "name": "Lakeside 3",
#         "ra_name": "Site C"
#     },
#     {
#         "site_id": "ce4d7c64-0e5a-11eb-adbd-0e9d39187912",
#         "listing_id": "5f999d87809958002ef4575d",
#         "state": "Louisiana",
#         "park": "Fontainebleau State Park",
#         "name": "Lakeside 4",
#         "ra_name": "Site D"
#     },
#     {
#         "site_id": "bf6110ee-0e5a-11eb-8a95-0e1671378d08",
#         "listing_id": "5f999d8b809958002ef45878",
#         "state": "Louisiana",
#         "park": "Fontainebleau State Park",
#         "name": "Teepee 1",
#         "ra_name": "Site E"
#     },
#     {
#         "site_id": "b95fb1c8-0e5a-11eb-8a95-0e1671378d08",
#         "listing_id": "5f999d8914e1410030b56f8e",
#         "state": "Louisiana",
#         "park": "Fontainebleau State Park",
#         "name": "Teepee 2",
#         "ra_name": "Site F"
#     },
#     {
#         "site_id": "bc2cc8be-0e5a-11eb-adbd-0e9d39187912",
#         "listing_id": "5f99b6e4809958002ef6d122",
#         "state": "Louisiana",
#         "park": "Fontainebleau State Park",
#         "name": "Teepee 3",
#         "ra_name": "Site H"
#     },
#     {
#         "site_id": "cacf26a0-0e5a-11eb-8a95-0e1671378d08",
#         "listing_id": "5f999d8c85efd600307f9390",
#         "state": "Louisiana",
#         "park": "Fontainebleau State Park",
#         "name": "Greenpoint 1",
#         "ra_name": "Site I"
#     },
#     {
#         "site_id": "c8062874-0e5a-11eb-adbd-0e9d39187912",
#         "listing_id": "5f999d8a809958002ef45872",
#         "state": "Louisiana",
#         "park": "Fontainebleau State Park",
#         "name": "Greenpoint 2",
#         "ra_name": "Site J"
#     },
#     {
#         "site_id": "869ae83e-0e5a-11eb-adbd-0e9d39187912",
        "listing_id": "5f999d8c85efd600307f9315",
        "state": "Louisiana",
        "park": "Fontainebleau State Park",
        "name": "Greenpoint 3",
        "ra_name": "Site G"
    },
    {
        "site_id": "70ae1f34-0fe3-11eb-adbd-0e9d39187912",
        "listing_id": "5f99a44aa5ba9b002fe3a4e1",
        "state": "Louisiana",
        "park": "Lake D'Arbonne State Park",
        "name": "Double 1",
        "ra_name": "Site G"
    },
#     {
#         "site_id": "6dfc2466-0fe3-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a44a14e1410030b5dc87",
#         "state": "Louisiana",
#         "park": "Lake D'Arbonne State Park",
#         "name": "Double 2",
#         "ra_name": "Site H"
#     },
#     {
#         "site_id": "7d8e3bda-0fe3-11eb-8a95-0e1671378d08",
#         "listing_id": "5f8a00471c0858002b1534ee",
#         "state": "Louisiana",
#         "park": "Lake D'Arbonne State Park",
#         "name": "Single 1",
#         "ra_name": "Site A"
#     },
#     {
#         "site_id": "7ffd3254-0fe3-11eb-adbd-0e9d39187912",
#         "listing_id": "5f99a44801b375002e740d30",
#         "state": "Louisiana",
#         "park": "Lake D'Arbonne State Park",
#         "name": "Single 2",
#         "ra_name": "Site B"
#     },
#     {
#         "site_id": "7b1a3f70-0fe3-11eb-adbd-0e9d39187912",
#         "listing_id": "5f99a44c809958002ef4ea51",
#         "state": "Louisiana",
#         "park": "Lake D'Arbonne State Park",
#         "name": "Single 3",
#         "ra_name": "Site C"
#     },
#     {
#         "site_id": "787f1e84-0fe3-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a44b14e1410030b5dd05",
#         "state": "Louisiana",
#         "park": "Lake D'Arbonne State Park",
#         "name": "Single 4",
#         "ra_name": "Site D"
#     },
#     {
#         "site_id": "762bedec-0fe3-11eb-adbd-0e9d39187912",
#         "listing_id": "5f99a44b14e1410030b5dd7c",
#         "state": "Louisiana",
#         "park": "Lake D'Arbonne State Park",
#         "name": "Single 5",
#         "ra_name": "Site E"
#     },
#     {
#         "site_id": "737b5c90-0fe3-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a44ba5ba9b002fe3a565",
#         "state": "Louisiana",
#         "park": "Lake D'Arbonne State Park",
#         "name": "Single 6",
#         "ra_name": "Site F"
#     },
#     {
#         "site_id": "086b5b2e-0fea-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a25685efd600307ff871",
#         "state": "Louisiana",
#         "park": "Lake Claiborne State Park",
#         "name": "Kayak 1",
#         "ra_name": "Site F"
#     },
#     {
#         "site_id": "0b0241f4-0fea-11eb-adbd-0e9d39187912",
#         "listing_id": "5f99a25514e1410030b5c6a4",
#         "state": "Louisiana",
#         "park": "Lake Claiborne State Park",
#         "name": "Kayak 2",
#         "ra_name": "Site G"
#     },
#     {
#         "site_id": "fe91b6c0-0fe9-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a25301b375002e73f0e3",
#         "state": "Louisiana",
#         "park": "Lake Claiborne State Park",
#         "name": "Kayak 3",
#         "ra_name": "Site H"
#     },
#     {
#         "site_id": "038e5868-0fea-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a25469a551002e02ba33",
#         "state": "Louisiana",
#         "park": "Lake Claiborne State Park",
#         "name": "Kayak 4",
#         "ra_name": "Site I"
#     },
#     {
#         "site_id": "f9e90128-0fe9-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a25414e1410030b5c5a9",
#         "state": "Louisiana",
#         "park": "Lake Claiborne State Park",
#         "name": "Lakefront double",
#         "ra_name": "Site E"
#     },
#     {
#         "site_id": "010228f4-0fea-11eb-adbd-0e9d39187912",
#         "listing_id": "5f8f42e2c7a109002cea15e7",
#         "state": "Louisiana",
#         "park": "Lake Claiborne State Park",
#         "name": "Single 1",
#         "ra_name": "Site A"
#     },
#     {
#         "site_id": "fc928de0-0fe9-11eb-adbd-0e9d39187912",
#         "listing_id": "5f99a25414e1410030b5c620",
#         "state": "Louisiana",
#         "park": "Lake Claiborne State Park",
#         "name": "Single 2",
#         "ra_name": "Site B"
#     },
#     {
#         "site_id": "05ce17bc-0fea-11eb-adbd-0e9d39187912",
#         "listing_id": "5f99a25769a551002e02baaa",
#         "state": "Louisiana",
#         "park": "Lake Claiborne State Park",
#         "name": "Single 3",
#         "ra_name": "Site C"
#     },
#     {
#         "site_id": "0d922ff6-0fea-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a25469a551002e02b9bc",
#         "state": "Louisiana",
#         "park": "Lake Claiborne State Park",
#         "name": "Single 4",
#         "ra_name": "Site D"
#     },
#     {
#         "site_id": "06c3896c-12fc-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a0d4a5ba9b002fe372cd",
#         "state": "Louisiana",
#         "park": "Jimmie Davis State Park",
#         "name": "Single 1",
#         "ra_name": "Site A"
#     },
#     {
#         "site_id": "04137560-12fc-11eb-adbd-0e9d39187912",
#         "listing_id": "5f99a0d385efd600307fe057",
#         "state": "Louisiana",
#         "park": "Jimmie Davis State Park",
#         "name": "Single 2",
#         "ra_name": "Site D"
#     },
#     {
#         "site_id": "f24856ac-12fb-11eb-adbd-0e9d39187912",
#         "listing_id": "5f99a0d501b375002e73dee8",
#         "state": "Louisiana",
#         "park": "Jimmie Davis State Park",
#         "name": "Single 3",
#         "ra_name": "Site E"
#     },
#     {
#         "site_id": "f03a1e2c-12fb-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a0d385efd600307fe05d",
#         "state": "Louisiana",
#         "park": "Jimmie Davis State Park",
#         "name": "Single 4",
#         "ra_name": "Site F"
#     },
#     {
#         "site_id": "fba18fd4-12fb-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a0d401b375002e73de71",
#         "state": "Louisiana",
#         "park": "Jimmie Davis State Park",
#         "name": "Single 5",
#         "ra_name": "Site G"
#     },
#     {
#         "site_id": "00d81c8e-12fc-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a0d414e1410030b5b36a",
#         "state": "Louisiana",
#         "park": "Jimmie Davis State Park",
#         "name": "Double 1",
#         "ra_name": "Site B"
#     },
#     {
#         "site_id": "eda84530-12fb-11eb-adbd-0e9d39187912",
#         "listing_id": "5f9062fc1375e2002570cc83",
#         "state": "Louisiana",
#         "park": "Jimmie Davis State Park",
#         "name": "Double 2",
#         "ra_name": "Site C"
#     },
#     {
#         "site_id": "09eeb6d4-13c9-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a6e714e1410030b616cd",
#         "state": "Louisiana",
#         "park": "South Toledo Bend State Park",
#         "name": "Site A",
#         "ra_name": "Site A"
#     },
#     {
#         "site_id": "0c51871c-13c9-11eb-adbd-0e9d39187912",
#         "listing_id": "5f99a6e569a551002e030cfc",
#         "state": "Louisiana",
#         "park": "South Toledo Bend State Park",
#         "name": "Site B",
#         "ra_name": "Site B"
#     },
#     {
#         "site_id": "0ee71604-13c9-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a6e714e1410030b61656",
#         "state": "Louisiana",
#         "park": "South Toledo Bend State Park",
#         "name": "Site C",
#         "ra_name": "Site C"
#     },
#     {
#         "site_id": "11e0399e-13c9-11eb-adbd-0e9d39187912",
#         "listing_id": "5f91fc232413f80024ffe858",
#         "state": "Louisiana",
#         "park": "South Toledo Bend State Park",
#         "name": "Site D",
#         "ra_name": "Site D"
#     },
#     {
#         "site_id": "fed92a5e-13c8-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a6e985efd60030805e6c",
#         "state": "Louisiana",
#         "park": "South Toledo Bend State Park",
#         "name": "Site E",
#         "ra_name": "Site E"
#     },
#     {
#         "site_id": "0702c1ea-13c9-11eb-adbd-0e9d39187912",
#         "listing_id": "5f99a6e8a5ba9b002fe3ca30",
#         "state": "Louisiana",
#         "park": "South Toledo Bend State Park",
#         "name": "Site F",
#         "ra_name": "Site F"
#     },
#     {
#         "site_id": "143be6b6-13c9-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a6e6a5ba9b002fe3c9b8",
#         "state": "Louisiana",
#         "park": "South Toledo Bend State Park",
#         "name": "Site G",
#         "ra_name": "Site G"
#     },
#     {
#         "site_id": "04743a12-13c9-11eb-8a95-0e1671378d08",
#         "listing_id": "5f99a6e714e1410030b616d3",
#         "state": "Louisiana",
#         "park": "South Toledo Bend State Park",
#         "name": "Site H",
#         "ra_name": "Site H"
#     },
#     {
#         "site_id": "01d9c4e8-13c9-11eb-adbd-0e9d39187912",
#         "listing_id": "5f99a6e7d4e5b6002f5db27a",
#         "state": "Louisiana",
#         "park": "South Toledo Bend State Park",
#         "name": "Site I",
#         "ra_name": "Site I"
#     },
#     {
#         "site_id": "167f224e-13c9-11eb-adbd-0e9d39187912",
#         "listing_id": "5f91ea265b99f40023fa1290",
#         "state": "Louisiana",
#         "park": "South Toledo Bend State Park",
#         "name": "Site J Double",
#         "ra_name": "Site J"
#     },
#     {
#         "site_id": "dfe94a8c-14a6-11eb-8a95-0e1671378d08",
#         "listing_id": "5f93838913f27000237d2576",
#         "state": "Louisiana",
#         "park": "Chicot State Park",
#         "name": "Site A",
#         "ra_name": "Site A"
#     },
#     {
#         "site_id": "d97cdbb4-14a6-11eb-8a95-0e1671378d08",
#         "listing_id": "5f999c9a14e1410030b53335",
#         "state": "Louisiana",
#         "park": "Chicot State Park",
#         "name": "Site D",
#         "ra_name": "Site D"
#     },
#     {
#         "site_id": "e3125bd6-14a6-11eb-adbd-0e9d39187912",
#         "listing_id": "5f999c9a69a551002e02359d",
#         "state": "Louisiana",
#         "park": "Chicot State Park",
#         "name": "Site E",
#         "ra_name": "Site E"
#     },
#     {
#         "site_id": "e869fe7c-14a6-11eb-adbd-0e9d39187912",
#         "listing_id": "5f999c9bd4e5b6002f5c840a",
#         "state": "Louisiana",
#         "park": "Chicot State Park",
#         "name": "Site F",
#         "ra_name": "Site F"
#     },
#     {
#         "site_id": "e5cfa676-14a6-11eb-8a95-0e1671378d08",
#         "listing_id": "5f999c9814e1410030b532ab",
#         "state": "Louisiana",
#         "park": "Chicot State Park",
#         "name": "Site B",
#         "ra_name": "Site B"
#     },
#     {
#         "site_id": "dd05f5cc-14a6-11eb-adbd-0e9d39187912",
#         "listing_id": "5f999c9c14e1410030b533db",
#         "state": "Louisiana",
#         "park": "Chicot State Park",
#         "name": "Site C",
#         "ra_name": "Site C"
#     },
#     {
#         "site_id": "65f46e1e-154b-11eb-adbd-0e9d39187912",
#         "listing_id": "5f9863d2538573002d0620e2",
#         "state": "Louisiana",
#         "park": "Lake Fausse State Park",
#         "name": "Double 1",
#         "ra_name": "Site A"
#     },
#     {
#         "site_id": "0a0523f4-1880-11eb-adbe-0e9d39187912",
#         "listing_id": "5f9869c29716ce00303a2efa",
#         "state": "Louisiana",
#         "park": "Grand Isle State Park",
#         "name": "Beachfront Campsite",
#         "ra_name": "Site A"
#     }
# ]

# payload = [
#     {
#         "site_id": "",
#         "listing_id": "",
#         "state": "Utah",
#         "park": "Piute State Park",
#         "name": "Single Site A",
#         "ra_name": "Site A"
#     },
#     {
#         "site_id": "",
#         "listing_id": "",
#         "state": "Utah",
#         "park": "Piute State Park",
#         "name": "Single Site B",
#         "ra_name": "Site B"
#     },
#     {
#         "site_id": "",
#         "listing_id": "",
#         "state": "Utah",
#         "park": "Piute State Park",
#         "name": "Single Site C",
#         "ra_name": "Site C"
#     },
#     {
#         "site_id": "",
#         "listing_id": "",
#         "state": "Utah",
#         "park": "Piute State Park",
#         "name": "Double Site D",
#         "ra_name": "Site D"
#     }
# ]

def clean_scrub_dict(d):
    """ Generic function cleaning JSON unsuported datatypes.
        This will lookUp the entire dictionary, iterate its values
        and replace the datatype for a supported representation.

        Parameters:

        d (Dictionary):
    """

    if type(d) is dict:
        return dict((k, clean_scrub_dict(v)) for k, v in d.items() if v and clean_scrub_dict(v))
    else:
        if isinstance(d, (datetime)):
            return d.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        elif isinstance(d, uuid.UUID):
            return str(d)
        elif isinstance(d, list):
            for index, el in enumerate(d):
                d[index] = clean_scrub_dict(el)
            return d
        else:
            return d

def write_to_table(table, entry):
    entry["last_updated"] = datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S")
    encoded = json.JSONEncoder().encode(entry)
    ddb_record = json.loads(encoded, parse_float=Decimal)

    print("writing to dynamo...")
    response = table.put_item(Item=ddb_record)
    print(response)
    return

def main():
    client = resource("dynamodb", region_name='us-east-1')
    table = client.Table("state_park_sites_PROD")
    for entry in payload:
        write_to_table(table, entry)
    return

if __name__ == "__main__":
    main()