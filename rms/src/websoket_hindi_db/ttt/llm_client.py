from openai import OpenAI 
import time
from typing import List, Dict, Tuple, Optional

from openai import OpenAI 
import time
from typing import List, Dict, Tuple, Optional

class LLMClient:
    """
    Handles all LLM interactions (prompting + function calling).
    Keeps assistant.py clean.
    """

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o-mini",
        max_tokens: int = 400,
        temperature: float = 0.5,
        language: str = "en"
    ):
        if not api_key:
            raise ValueError("OPENAI_API_KEY is missing.")
        self.openai_client = OpenAI(api_key=api_key)
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.history: List[Dict] = []
        self.current_language = language
        self.system_prompts = self._get_system_prompts()
        self.system_prompt = self.system_prompts.get(language, self.system_prompts["en"])

    def _get_system_prompts(self) -> Dict[str, str]:
        """Return language-specific system prompts"""
        return {
            "en": """You are a helpful AI assistant. Always respond in English.
                    Be polite, friendly, and helpful. Keep responses concise but informative.
                    You are a restaurant assistant for Infocall Dine.

                    STRICT RULES:
                    1. Respond ONLY in English
                    2. Keep responses VERY SHORT (max 1–2 sentences)
                    3. Mention prices using ₹ symbol
                    4. Be polite, warm, and professional
                    5. DO NOT explain reasoning or internal steps
                    6. DO NOT mention systems, tools, functions, or logic
                    7. NEVER invent prices, dishes, or menu items
                    8. ONLY speak based on information explicitly provided to you
                    9. If information is missing or unclear, politely say you will check
                    10. Do NOT include emojis, labels, or formatting symbols
                    11. ALWAYS confirm orders before finalizing
                    12. NEVER ask for feedback or ratings
                    13. NEVER refer to yourself as an AI model or assistant
                    14. NEVER mention technical issues or errors""",
            
            "hi": """आप एक सहायक AI हैं। हमेशा हिंदी में जवाब दें। 
                    विनम्र, मिलनसार और सहायक रहें। जवाब संक्षिप्त लेकिन जानकारीपूर्ण रखें। 
                    आप इन्फोकॉल डाइन के लिए रेस्टोरेंट असिस्टेंट हैं।

                    कड़े नियम:
                    1. केवल हिंदी में जवाब दें
                    2. जवाब बहुत संक्षिप्त रखें (अधिकतम 1-2 वाक्य)
                    3. कीमतों का उल्लेख ₹ प्रतीक का उपयोग करके करें
                    4. विनम्र, गर्मजोशी भरा और पेशेवर रहें
                    5. तर्क या आंतरिक चरणों की व्याख्या न करें
                    6. सिस्टम, टूल, फंक्शन या लॉजिक का उल्लेख न करें
                    7. कभी भी कीमतें, व्यंजन या मेनू आइटम का आविष्कार न करें
                    8. केवल स्पष्ट रूप से प्रदान की गई जानकारी के आधार पर बोलें
                    9. यदि जानकारी गायब है या अस्पष्ट है, तो विनम्रता से कहें कि आप जाँच करेंगे
                    10. इमोजी, लेबल या फ़ॉर्मेटिंग प्रतीक शामिल न करें
                    11. अंतिम रूप देने से पहले हमेशा ऑर्डर की पुष्टि करें
                    12. कभी भी प्रतिक्रिया या रेटिंग न मांगें
                    13. कभी भी खुद को AI मॉडल या असिस्टेंट न बताएं
                    14. कभी भी तकनीकी समस्याओं या त्रुटियों का उल्लेख न करें""",
            
            # "gu": """તમે એક સહાયક AI છો. હંમેશા ગુજરાતીમાં જવાબ આપો.
            #         વિનમ્ર, મિત્રતાપૂર્ણ અને સહાયક રહો. જવાબ સંક્ષિપ્ત પરંતુ માહિતીપ્રદ રાખો.
            #         તમે ઇન્ફોકોલ ડાઇન માટે રેસ્ટોરન્ટ સહાયક છો।

            #         કડક નિયમો:
            #         1. ફક્ત ગુજરાતીમાં જ જવાબ આપો
            #         2. જવાબ ખૂબ જ ટૂંકો રાખો (મહત્તમ 1-2 વાક્ય)
            #         3. કિંમતોનો ઉલ્લેખ ₹ ચિન્હનો ઉપયોગ કરીને કરો
            #         4. વિનમ્ર, ગરમજોશી ભર્યું અને પેશેવર રહો
            #         5. તર્ક અથવા આંતરિક પગલાઓ સમજાવશો નહીં
            #         6. સિસ્ટમ, ટૂલ્સ, ફંક્શન અથવા લૉજિકનો ઉલ્લેખ ન કરો
            #         7. કદી પણ કિંમતો, વાનગીઓ અથવા મેનુ આઇટમ્સની શોધ ન કરો
            #         8. ફક્ત સ્પષ્ટ રીતે પ્રદાન કરેલી માહિતીના આધારે બોલો
            #         9. જો માહિતી ખૂટે છે અથવા અસ્પષ્ટ છે, તો વિનંતીપૂર્વક કહો કે તમે તપાસ કરશો
            #         10. ઇમોજી, લેબલ અથવા ફોર્મેટિંગ ચિન્હો શામેલ ન કરો
            #         11. અંતિમ રૂપ આપતા પહેલા હંમેશા ઓર્ડરની પુષ્ટિ કરો
            #         12. કદી પણ પ્રતિસાદ અથવા રેટિંગ માંગશો નહીં
            #         13. કદી પણ તમારી જાતને AI મોડલ અથવા સહાયક તરીકે ન બતાવો
            #         14. કદી પણ તકનીકી સમસ્યાઓ અથવા ભૂલોનો ઉલ્લેખ ન કરો"""
        
        }

    def reset(self) -> None:
        """Reset the conversation history."""
        self.history = []

    def set_language(self, language: str) -> None:
        """Update the current language and system prompt."""
        if language in self.system_prompts:
            self.current_language = language
            self.system_prompt = self.system_prompts[language]
        else:
            self.current_language = "en"
            self.system_prompt = self.system_prompts["en"]

    def _build_messages(self, max_history: Optional[int] = None) -> List[Dict]:
        """
        Build messages for the API call.
        """
        messages = [{"role": "system", "content": self.system_prompt}]
        
        # Add conversation history (last N messages if max_history specified)
        history_to_include = self.history
        if max_history and max_history > 0:
            history_to_include = self.history[-max_history:]
        
        messages.extend(history_to_include)
        
        return messages

    def chat(self, user_text: str, max_history: Optional[int] = None) -> Tuple[str, float]:
        """
        Interacts with the OpenAI API and returns the assistant's response.
        """
        # Add user message to history
        self.history.append({"role": "user", "content": user_text})

        start = time.time()

        # Get messages with history limit
        messages = self._build_messages(max_history)
        
        # Make API call
        response = self.openai_client.chat.completions.create(
            model=self.model,
            messages=messages,
            max_tokens=self.max_tokens,
            temperature=self.temperature
        )

        assistant_text = (response.choices[0].message.content or "").strip()
        elapsed = time.time() - start

        # Add assistant response to history
        self.history.append({"role": "assistant", "content": assistant_text})

        return assistant_text, elapsed
# class LLMClient:
#     """
#     Handles all LLM interactions (prompting + function calling).
#     Keeps assistant.py clean.
#     """

#     def __init__(
#         self,
#         api_key: str,
#         model: str = "gpt-4o-mini",
#         max_tokens: int = 400,
#         temperature: float = 0.5,
#         system_prompt: str = None
#     ):
#         if not api_key:
#             raise ValueError("OPENAI_API_KEY is missing.")
#         self.openai_client = OpenAI(api_key=api_key)  # Fixed: use api_key parameter

#         self.model = model
#         self.max_tokens = max_tokens
#         self.temperature = temperature
#         self.history: List[Dict] = []
#         self.system_prompt = system_prompt or self._default_system_prompt()

#     def _default_system_prompt(self) -> str:
#         """Default system prompt if none provided."""
#         return """
#         You are a restaurant assistant for Infocall Dine.

#         STRICT RULES:
#         1. Respond ONLY in the user's language (Hindi / English / Gujarati)
#         2. Keep responses VERY SHORT (max 1–2 sentences)
#         3. Use Devanagari for Hindi, Gujarati script for Gujarati
#         4. Mention prices using ₹ symbol
#         5. Be polite, warm, and professional
#         6. DO NOT explain reasoning or internal steps
#         7. DO NOT mention systems, tools, functions, or logic
#         8. NEVER invent prices, dishes, or menu items
#         9. ONLY speak based on information explicitly provided to you
#         10. If information is missing or unclear, politely say you will check
#         11. Do NOT include emojis, labels, or formatting symbols
#         12. ALWAYS confirm orders before finalizing
#         13. NEVER ask for feedback or ratings
#         14. NEVER refer to yourself as an AI model or assistant
#         15. NEVER mention technical issues or errors

#         For Gujarati responses:
#         - Always use Gujarati script (not Roman transliteration)
#         - Use polite phrases like "મહેરબાની કરીને", "આભાર", "કૃપા કરી"
#         - Address customer as "તમે" (informal) or "તમારે" (formal)

#         For Hindi responses:
#         - Use polite phrases like "कृपया", "धन्यवाद", "शुक्रिया"
#         - Address customer as "आप"

#         For English responses:
#         - Use polite phrases like "please", "thank you", "kindly"
#         """

#     def reset(self) -> None:
#         """Reset the conversation history."""
#         self.history = []

#     def _build_messages(
#         self, 
#         context: Optional[str] = None, 
#         max_history: Optional[int] = None
#     ) -> List[Dict]:
#         """
#         Build messages for the API call.
        
#         Args:
#             context: Additional context to prepend to system prompt
#             max_history: Maximum number of historical messages to include (last N)
#         """
#         # Combine system prompt with context if provided
#         system_content = self.system_prompt
#         if context:
#             system_content = context + system_content
        
#         messages = [{"role": "system", "content": system_content}]
        
#         # Add conversation history (last N messages if max_history specified)
#         history_to_include = self.history
#         if max_history and max_history > 0:
#             history_to_include = self.history[-max_history:]
        
#         messages.extend(history_to_include)
        
#         return messages

#     def chat(
#         self, 
#         user_text: str, 
#         context: Optional[str] = None,
#         max_history: Optional[int] = None
#     ) -> Tuple[str, float]:
#         """
#         Interacts with the OpenAI API and returns the assistant's response.
        
#         Args:
#             user_text: User's message
#             context: Additional context to prepend to system prompt
#             max_history: Maximum number of historical messages to include
            
#         Returns: (assistant_text, elapsed_seconds)
#         """
#         # Add user message to history
#         self.history.append({"role": "user", "content": user_text})

#         start = time.time()

#         # Get messages with context and history limit
#         messages = self._build_messages(context, max_history)
        
#         # Make API call using the new OpenAI SDK syntax
#         response = self.openai_client.chat.completions.create(
#             model=self.model,
#             messages=messages,
#             max_tokens=self.max_tokens,
#             temperature=self.temperature
#         )

#         assistant_text = (response.choices[0].message.content or "").strip()
#         elapsed = time.time() - start

#         # Add assistant response to history
#         self.history.append({"role": "assistant", "content": assistant_text})

#         return assistant_text, elapsed

#     def set_system_prompt(self, system_prompt: str) -> None:
#         """Update the system prompt."""
#         self.system_prompt = system_prompt

# import json
# import time
# from typing import List, Dict, Optional, Tuple

# import openai
# from websoket_hindi.ttt.prompts import SYSTEM_PROMPT
# from gpt_with_functions import FUNCTIONS, execute_function, format_function_result_for_gpt

# class LLMClient:
#     """
#     Handles all LLM interactions (prompting + function calling).
#     Keeps assistant.py clean.
#     """

#     def __init__(
#         self,
#         api_key: str,
#         model: str = "gpt-3.5-turbo",
#         max_tokens: int = 150,
#         temperature: float = 0.5,
#         history_max_messages: int = 6,  # last 3 exchanges
#     ):
#         if not api_key:
#             raise ValueError("OPENAI_API_KEY is missing.")
#         openai.api_key = api_key

#         self.model = model
#         self.max_tokens = max_tokens
#         self.temperature = temperature
#         self.history_max_messages = history_max_messages

#         self.history: List[Dict] = []

#     def reset(self) -> None:
#         self.history = []

#     def _trim_history(self) -> None:
#         if len(self.history) > self.history_max_messages:
#             self.history = self.history[-self.history_max_messages :]

#     def add_user(self, text: str) -> None:
#         self.history.append({"role": "user", "content": text})
#         self._trim_history()

#     def add_assistant(self, text: str) -> None:
#         self.history.append({"role": "assistant", "content": text})
#         self._trim_history()

#     def _build_messages(self) -> List[Dict]:
#         return [{"role": "system", "content": SYSTEM_PROMPT}, *self.history]

#     def chat(self, user_text: str) -> Tuple[str, Optional[str], float]:
#         """
#         Returns: (assistant_text, function_name_if_any, elapsed_seconds)
#         """
#         self.add_user(user_text)

#         start = time.time()

#         resp = openai.ChatCompletion.create(
#             model=self.model,
#             messages=self._build_messages(),
#             functions=FUNCTIONS,
#             function_call="auto",
#             max_tokens=self.max_tokens,
#             temperature=self.temperature,
#         )

#         msg = resp.choices[0].message
#         function_name = None

#         # If function call requested
#         if msg.get("function_call"):
#             function_name = msg["function_call"]["name"]
#             raw_args = msg["function_call"].get("arguments", "{}")

#             try:
#                 function_args = json.loads(raw_args) if raw_args else {}
#             except json.JSONDecodeError:
#                 # Hard fallback: treat as empty args
#                 function_args = {}

#             function_result = execute_function(function_name, function_args)
#             formatted_result = format_function_result_for_gpt(function_name, function_result)

#             # Push function result
#             self.history.append(
#                 {"role": "function", "name": function_name, "content": formatted_result}
#             )
#             self._trim_history()

#             # Second model call
#             resp2 = openai.ChatCompletion.create(
#                 model=self.model,
#                 messages=self._build_messages(),
#                 max_tokens=self.max_tokens,
#                 temperature=self.temperature,
#             )
#             assistant_text = resp2.choices[0].message.content.strip()
#         else:
#             assistant_text = (msg.content or "").strip()

#         elapsed = time.time() - start
#         self.add_assistant(assistant_text)

#         return assistant_text, function_name, elapsed
