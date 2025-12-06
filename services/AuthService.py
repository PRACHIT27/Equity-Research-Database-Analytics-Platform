"""
Authentication Service Layer
Handles user authentication, authorization, and permission management
NO SQL QUERIES - All database access through repository
"""

from typing import Optional, Dict, Any
from repositories.UserRepository import UserRepository
from utils.validators import UserValidator
from utils.exceptions import AuthenticationError, ValidationError

class AuthService:
    """
    Service for authentication and authorization.
    Handles login, permission checking, password management.
    Focused on SECURITY operations only.
    """

    def __init__(self, user_repo: UserRepository):
        """
        Initialize auth service.

        Args:
            user_repo: UserRepository instance
        """
        self.user_repo = user_repo
        self.validator = UserValidator()

    # ========== AUTHENTICATION ==========

    def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticate user with username and password.
        Uses stored procedure via repository.

        Args:
            username: Username
            password: Plain text password

        Returns:
            dict: User data with permissions or None if auth fails

        Raises:
            ValidationError: If inputs are invalid
            AuthenticationError: If account is deactivated
        """
        # Validate inputs
        if not username or not password:
            raise ValidationError("Username and password are required")

        if len(username) < 3:
            raise ValidationError("Username must be at least 3 characters")

        if len(password) < 6:
            raise ValidationError("Password must be at least 6 characters")

        # Authenticate using repository (NO SQL here - repo handles it!)
        user_data = self.user_repo.authenticate(username, password)

        if not user_data:
            # Don't specify whether username or password was wrong (security)
            return None

        # Check if user is active
        if not user_data.get('is_active', False):
            raise AuthenticationError("User account is deactivated. Contact administrator.")

        return user_data

    # ========== AUTHORIZATION (Permission Checking) ==========

    def check_permission(self, user_id: int, permission_type: str) -> bool:
        """
        Check if user has specific permission.
        Uses database function via repository.

        Args:
            user_id: User ID
            permission_type: Permission to check (CREATE, READ, UPDATE, DELETE, etc.)

        Returns:
            bool: True if user has permission
        """
        try:
            # Call database function through repository (NO SQL here!)
            result = self.user_repo.call_function('HasPermission', (user_id, permission_type))
            return bool(result)
        except Exception:
            # If error, deny permission (fail-safe)
            return False

    def get_user_permission_level(self, user_id: int) -> int:
        """
        Get user's permission level (1-10).
        Uses database function via repository.

        Args:
            user_id: User ID

        Returns:
            int: Permission level (1=View Only, 10=Full Access)
        """
        try:
            # Call database function through repository (NO SQL here!)
            result = self.user_repo.call_function('GetUserPermissionLevel', (user_id,))
            return int(result) if result else 0
        except Exception:
            # If error, return lowest level (fail-safe)
            return 0

    def get_user_permissions(self, user_id: int) -> Dict[str, bool]:
        """
        Get all permissions for a user.

        Args:
            user_id: User ID

        Returns:
            dict: Dictionary of permission flags
        """
        permissions = {
            'can_create': self.check_permission(user_id, 'CREATE'),
            'can_read': self.check_permission(user_id, 'READ'),
            'can_update': self.check_permission(user_id, 'UPDATE'),
            'can_delete': self.check_permission(user_id, 'DELETE'),
            'can_execute_reports': self.check_permission(user_id, 'REPORT'),
            'can_manage_users': self.check_permission(user_id, 'MANAGE_USERS'),
            'can_approve': self.check_permission(user_id, 'APPROVE')
        }

        return permissions

    def require_permission(self, user_id: int, permission_type: str):
        """
        Require specific permission or raise error.

        Args:
            user_id: User ID
            permission_type: Required permission

        Raises:
            AuthenticationError: If user lacks permission
        """
        if not self.check_permission(user_id, permission_type):
            raise AuthenticationError(
                f"You don't have {permission_type} permission. Contact your administrator."
            )

    # ========== PASSWORD MANAGEMENT ==========

    def change_password(self, user_id: int, old_password: str, new_password: str) -> Dict[str, Any]:
        """
        Change user password with validation.

        Args:
            user_id: User ID
            old_password: Current password
            new_password: New password

        Returns:
            dict: Success message

        Raises:
            ValidationError: If passwords invalid
            AuthenticationError: If old password wrong
        """
        # Get user through repository (NO SQL here!)
        user = self.user_repo.find_by_id_full(user_id)

        if not user:
            raise AuthenticationError("User not found")

        # Verify old password
        import bcrypt
        if not bcrypt.checkpw(old_password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            raise AuthenticationError("Current password is incorrect")

        # Validate new password
        self.validator.validate_password(new_password)

        if new_password == old_password:
            raise ValidationError("New password must be different from current password")

        # Update password through repository (NO SQL here!)
        self.user_repo.update_password(user_id, new_password)

        return {
            'success': True,
            'message': 'Password changed successfully. Please login again with new password.'
        }

    def reset_password(self, user_id: int, new_password: str, admin_user_id: int) -> Dict[str, Any]:
        """
        Reset user password (admin only).

        Args:
            user_id: User ID whose password to reset
            new_password: New password
            admin_user_id: Admin performing the reset

        Returns:
            dict: Success message

        Raises:
            AuthenticationError: If requester is not admin
        """
        # Check admin has permission (uses function, no SQL!)
        if not self.check_permission(admin_user_id, 'MANAGE_USERS'):
            raise AuthenticationError("Only administrators can reset passwords")

        # Validate new password
        self.validator.validate_password(new_password)

        # Reset password through repository (NO SQL here!)
        self.user_repo.update_password(user_id, new_password)

        return {
            'success': True,
            'message': 'Password reset successfully. User should login with new password.'
        }

    # ========== SESSION MANAGEMENT ==========

    def update_last_login(self, user_id: int) -> bool:
        """
        Update user's last login timestamp.

        Args:
            user_id: User ID

        Returns:
            bool: Success status
        """
        try:
            # Call repository method (NO SQL here!)
            self.user_repo.update_last_login(user_id)
            return True
        except:
            return False

    def logout_user(self, user_id: int) -> Dict[str, Any]:
        """
        Handle user logout.

        Args:
            user_id: User ID

        Returns:
            dict: Success message
        """
        # Could log activity through repository
        # For now, just return success (no SQL needed!)
        return {
            'success': True,
            'message': 'Logged out successfully'
        }

    def validate_session(self, user_id: int) -> bool:
        """
        Validate if user session is still valid.

        Args:
            user_id: User ID

        Returns:
            bool: True if session valid
        """
        try:
            # Get user through repository (NO SQL here!)
            user = self.user_repo.find_by_id_full(user_id)

            if not user:
                return False

            return bool(user.get('is_active', False))
        except:
            return False