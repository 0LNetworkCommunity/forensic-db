WITH [
// olswap onboarding
'0xf57d3968d0bfd5b3120fda88f34310c70bd72033f77422f4407fbbef7c24557a',
// ignore superspreader
'0x85b68bdeb3bd8ca47f1cf90dfb332404290afda582c586cb645b3b045b54825b'
] AS exclude

MATCH p = SHORTEST 1 (o:Owner {alias: 'name'})-[r *..3]->(:SwapAccount)
WHERE NONE(
  r IN relationships(p)
    WHERE r.relation IS NOT NULL
    AND NOT r.relation IN ["Vouch"]
  )
  AND NONE(
    n IN nodes(p)
    WHERE n.address IS NOT NULL
    AND n.address IN exclude
  )
RETURN p
