prompt = """
You are a tool that determines if text discusses policy and what policy area is discussed. 
Say "yes" if the text contains discussion of a policy.  Discussion of policy can include discussion of specific legislation or general discussion such as healthcare, education, environment, foreign policy, the economy, defense spending, national security, etc.  Otherwise say "no".
The following is a list of policy areas and their definitions.

"Agriculture and Food": agricultural practices; agricultural prices and marketing; agricultural education; food assistance or nutrition programs; food industry, supply, and safety; aquaculture; horticulture and plants. 
"Armed Forces and National Security": military operations and spending, facilities, procurement and weapons, personnel, intelligence; strategic materials; war and emergency powers; veterans’ issues. 
"Civil Rights and Liberties, Minority Issues": discrimination on basis of race, ethnicity, age, sex, gender, health or disability; First Amendment rights; due process and equal protection; abortion rights; privacy. 
"Commerce": business investment, development, regulation; small business; consumer affairs; competition and restrictive trade practices; manufacturing, distribution, retail; marketing; intellectual property. 
"Crime and Law Enforcement": criminal offenses, investigation and prosecution, procedure and sentencing; corrections and imprisonment; juvenile crime; law enforcement administration. 
"Economics and Public Finance": budgetary matters such as appropriations, public debt, the budget process, government lending, government accounts and trust funds; monetary policy and inflation; economic development, performance
"Education": elementary, secondary, or higher education including special education and matters of academic performance, school administration, teaching, educational costs, and student aid.
"Emergency Management": emergency planning; response to civil disturbances, natural and other disasters, including fires; emergency communications; security preparedness.
"Energy": all sources and supplies of energy, including alternative energy sources, oil and gas, coal, nuclear power; efficiency and conservation; costs, prices, and revenues; electric power transmission; public utility matters.a
"Environmental Protection": regulation of pollution including from hazardous substances and radioactive releases; climate change and greenhouse gases; environmental assessment and research; solid waste and recycling; ecology. 
"Families": child and family welfare, services, and relationships; marriage and family status; domestic violence and child abuse. 
"Finance and Financial Sector": U.S. banking and financial institutions regulation; consumer credit; bankruptcy and debt collection; financial services and investments; insurance; securities; real estate transactions; currency. 
"Foreign Trade and International Finance": competitiveness, trade barriers and adjustment assistance; foreign loans and international monetary system; international banking; trade agreements and negotiations; customs enforcement, tariffs, and trade restrictions; foreign investment. 
"Government Operations and Politics": government administration, including agency organization, contracting, facilities and property, information management and services; rulemaking and administrative law; elections and political activities; government employees and officials; Presidents; ethics and public participation; postal service. 
"Health": science or practice of the diagnosis, treatment, and prevention of disease; health services administration and funding, including such programs as Medicare and Medicaid; health personnel and medical education; drug use and safety; health care coverage and insurance; health facilities. 
"Housing and Community Development": home ownership; housing programs administration and funding; residential rehabilitation; regional planning, rural and urban development; affordable housing; homelessness; housing industry and construction; fair housing. 
"Immigration": administration of immigration and naturalization matters; immigration enforcement procedures; refugees and asylum policies; travel and residence documentation; foreign labor; benefits for immigrants. 
"International Affairs": matters affecting foreign aid, human rights, international law and organizations; national governance; arms control; diplomacy and foreign officials; alliances and collective security. 
"Labor and Employment": matters affecting hiring and composition of the workforce, wages and benefits, labor-management relations; occupational safety, personnel management, unemployment compensation. 
"Law": matters affecting civil actions and administrative remedies, courts and judicial administration, general constitutional issues, dispute resolution, including mediation and arbitration. 
"Public Lands and Natural Resources": natural areas (including wilderness); lands under government jurisdiction; land use practices and policies; parks, monuments, and historic sites; fisheries and marine resources; mining and minerals. 
"Science, Technology, Communications": natural sciences, space exploration, research policy and funding, research and development, STEM education, scientific cooperation and communication; technology policies, telecommunication, information technology; digital media, journalism. 
"Taxation": all aspects of income, excise, property, inheritance, and employment taxes; tax administration and collection. 
"Transportation and Public Works": all aspects of transportation modes and conveyances, including funding and safety matters; Coast Guard; infrastructure development; travel and tourism. 
"Water Resources Development": the supply and use of water and control of water flows; watersheds; floods and storm protection; wetlands. 

Also identify if the text discusses specific legislation.  This includes the consideration, development, or enactment of laws and regulations. It must reference a bill, law, resolution or rule (either directly by full name or by reference as a "law", "bill" or "resolution"). 
Provide your response in the following JSON format:
"is_policy" should contain your evaluation of the text (yes or no).
"policy_area" should include all policy areas that apply to the text from the list above, with each a value in the JSON node policy_area.
"legislative_discussion" should record if the text discusses specific legislation (yes or no)
"model_explanation" should contain your reasoning on if the text is policy or not.  It should be no more than 20 words.
{{
"is_policy": "",
"policy_area": "",
"legislative_discussion":"",
"model_explanation": ""
}}
Only provide JSON.
With this in mind, evaluate the following text: "{target}"
"""

def yesno(x):
    x = x.lower()
    if x == 'yes':
        return 1
    elif x == 'no':
        return 0
    else:
        return None

column_map = {
    'is_policy': {
        'name': 'policy',
        'filter': lambda x: yesno(x),
    },
    'policy_area': {
        'name': 'policy_area',
        'filter': lambda x: str(x),
    },
    'legislative_discussion': {
        'name': 'policy_legislative_discussion',
        'filter': lambda x: yesno(x),
    },
    'model_explanation': {
        'name': 'policy_explanation',
        'filter': lambda x: x,
    },
}