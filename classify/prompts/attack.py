prompt = """
You are a tool that determines if text makes a personal attack.
These attacks must be on a person or a political party. Political parties are called Democrats, Republicans, Conservatives or Liberals. Not all negative or derogatory language is an attack.
Only say "yes" if the text attacks the character, integrity, intelligence, morality or patriotism of a person or a political party. For example, calling a person or group evil, stupid, anti-american, or questioning patriotism or intelligence would be a yes. Only say yes if you are certain.
Say "no" if the text is a criticism of policy or legislation and not an attack. 
This is a personal attack because it is about a person (Donald Trump) and calls him a cult leader: "I get it. They don't like that their cult leader, Donald Trump, will go down in history as one of the worst Presidents we have ever had."
This is a personal attack because it is about Joe Biden and calls him the worst president in history: "I am going to start off with a compliment. He has been able to accomplish being the worst President and has replaced Jimmy Carter as the worst President this country has ever had of all time, and it is because of his policies."
This is not a personal attack because it is criticism of a policy and not a person: "The Republicans admitted out loud that they are holding the American economy hostage. It isn't about finding ‘a compromise,’ the only ‘concession’ they are willing to make is not causing a recession. It's extortion, plain and simple, and millions of American jobs are at stake."
This is not a personal attack because it is an attack on Hamas, which is a foreign terrorist group and not a person or a political party: "Hamas even fires rockets indiscriminately towards Israeli civilians, many of which misfire and land, killing people in their own Gaza, innocent Palestinians. Make no mistake, the only word to describe Hamas is ‘evil,’ and when they commit atrocities like those of October 7, we must not look away."
Otherwise say no. Also say "no" if the attack was about the leader of a country other than the United States (for example Xi Jinping, Vladimir Putin).
If text is not a personal attack, check to see if it is policy criticism. Policy criticism can express a strong negative opinion when criticizing a person's policy views or a policy.  
If the text expresses policy criticism, say "yes." if it does not say "no."  If you are not sure say "NA"
Provide your response in the following JSON format:
"is_attack" should contain your evaluation of the text (yes or no)
"is_person": indicate "yes" if the attack is made against a person (say "no" if the text is not an attack)
"is_criticism" should report if the text is a policy criticism (yes or no).
"attack_target" should contain the target of the attack (say NA if the text is not an attack)
"model_explanation" should contain your reasoning and all explanatory text from your response. 
Remember is_person must be "no" if is_attack is "no".
{{
"is_attack": "",
"is_person": "",
"is_criticism": "",
"attack_target": "",
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
    'is_attack': {
        'name': 'attack',
        'filter': lambda x: yesno(x),
    },
    'is_person': {
        'name': 'attack_personal',
        'filter': lambda x: yesno(x),
    },
    'is_criticism': {
        'name': 'attack_policy',
        'filter': lambda x: yesno(x),
    },
    'attack_target': {
        'name': 'attack_target',
        'filter': lambda x: x,
    },
    'model_explanation': {
        'name': 'attack_explanation',
        'filter': lambda x: x,
    },
}

