

import json
import random
from seleniumbase import SB

data = json.load(open('masterportal_scholarship.json', mode='r'))

links = [f"https://www.mastersportal.com/scholarships/{item['func']['getScholarshipId']}/{item['func']['getScholarshipVirtualName']}.html#content:eligibility" for item in data]
print(len(links))

links = links[93:]
result = []
with SB(uc=True) as sb:
    sb.open("https://www.google.com/gmail/about/")
    sb.click('a[data-action="sign in"]')
    sb.type('input[type="email"]', "haidt.221296.hust@gmail.com")
    sb.click('button:contains("Next")')
    # sb.sleep(10)
    # sb.type('input[type="password"]', "haidt261qaz")
    # sb.click('button:contains("Next")')

    sb.sleep(60)
    sb.open("https://www.mastersportal.com/account/my-journey/discover/")

    sb.sleep(30)

    for index, (link, item) in enumerate(zip(links, data)):
        print("index ", index+1)
        print(link)
        sb.open(link)
        sb.sleep(10)

        elements = sb.find_elements('#SwitchableContent > div.SwitchableGroup.js-content-eligibility.is-visible > article > div.Module.StudyPortals_Shared_Modules_Scholarship_Eligibility_Eligibility > section > article:nth-child(1) > ul')

        eli_text = ""
        for element in elements:
            print(element.text)
            eli_text += element.text

        result.append({
            "id": item['func']['getScholarshipId'],
            "eligibility": eli_text
        })

        sb.sleep(random.randint(3,5))

        with open("eligibility2.json", mode='w') as f:
            json.dump(result, f, indent=4, ensure_ascii=False)