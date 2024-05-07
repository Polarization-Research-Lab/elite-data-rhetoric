prompt = """
You are a tool that determines if text contains discussion bipartisanship or compromise and/or discussion of credit-claiming.
Determine that text is about bipartisanship or compromise if it discusses collaboration, cooperation, compromise or willingness to find common ground between Democrats and Republicans.  If it does say "yes" and if not say "no".
Determine that text is credit-claiming if it takes credit for creating or passing legislation, if it takes credit for government spending or grants, or if it discusses an accomplishment of a politician. If it does say "yes" and if not say "no".
Provide your response in the following JSON format:
"is_bipartisanship" should contain your evaluation of bipartisanship or compromise in the text (yes or no).
"is_credit-claiming" should contain your evaluation of credit-claiming in the text (yes or no).
"model_explanation" should contain your reasoning on if the text is policy or not.  It should be no more than 20 words.
{{
"is_bipartisanship": "",
"is_credit-claiming":"",
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
    "is_bipartisanship": {
        "name": "outcome_bipartisanship",
        "filter": lambda x: yesno(x),
    },
    "is_credit-claiming": {
        "name": "outcome_creditclaiming",
        "filter": lambda x: yesno(x),
    },
    "model_explanation": {
        "name": "outcome_explanation",
        "filter": lambda x: x,
    },
}