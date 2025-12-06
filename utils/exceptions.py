"""
Custom Exception Classes
Application-specific exceptions for better error handling
"""

class ValidationError(Exception):
    """Raised when input validation fails"""
    pass

class BusinessLogicError(Exception):
    """Raised when business rules are violated"""
    pass

class AuthenticationError(Exception):
    """Raised when authentication fails"""
    pass

class AuthorizationError(Exception):
    """Raised when user lacks permission"""
    pass

class DatabaseError(Exception):
    """Raised when database operation fails"""
    pass

class NotFoundError(Exception):
    """Raised when requested resource is not found"""
    pass

class DuplicateError(Exception):
    """Raised when trying to create duplicate record"""
    pass